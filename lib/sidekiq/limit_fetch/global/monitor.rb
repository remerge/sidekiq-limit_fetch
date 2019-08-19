module Sidekiq::LimitFetch::Global
  module Monitor
    extend self

    HEARTBEAT_PREFIX = 'limit:heartbeat:'
    PROCESS_SET = 'limit:processes'
    HEARTBEAT_TTL = 20
    REFRESH_TIMEOUT = 5

    def start!(ttl=HEARTBEAT_TTL, timeout=REFRESH_TIMEOUT)
      # We run this once syncronously so that callers can have more confidence
      # that the current process is "valid."
      run_limit_heartbeat ttl
      Thread.new do
        loop do
          run_limit_heartbeat ttl
          sleep timeout
        end
      end
    end

    def all_processes
      Sidekiq.redis {|it| it.smembers PROCESS_SET }
    end

    def old_processes
      all_processes.reject do |process|
        Sidekiq.redis {|it| it.get heartbeat_key process }
      end
    end

    def remove_old_processes!
      processes_to_remove = old_processes
      Sidekiq.redis do |it|
        processes_to_remove.each {|process| it.srem PROCESS_SET, process }
      end
      processes_to_remove
    end

    def add_dynamic_queues
      queues = Sidekiq::LimitFetch::Queues
      queues.add Sidekiq::Queue.all.map(&:name) if queues.dynamic?
    end

    private

    def update_heartbeat(ttl)
      Sidekiq.redis do |it|
        it.multi do
          it.set heartbeat_key, true
          it.sadd PROCESS_SET, Selector.uuid
          it.expire heartbeat_key, ttl
        end
      end
    end

    def note_current_probed_processes
      # This method is necessary to avoid a situation where we remove a process "OLDP" from the PROCESS_SET
      # but then our current process "CP" dies. Without this method, we would be stuck with some or all of OLDP's
      # existing locks forever. However, this method re-adds "OLDP" to the PROCESS_SET to
      # give us a chance to remove them if there is no heartbeat.
      #
      # This will not result in an infinite loop because the only thing that _adds_ process ids to the
      # probed locks is actual work. So, eventually, we'll remove all the bad locks from the probed lists,
      # and then remove those entries from the PROCESS_SET one last time.
      Sidekiq.redis do |it|
        current_probed_processes = Set.new
        Sidekiq::Queue.instances.each do |queue|
          current_probed_processes.merge(queue.lock.probed_processes)
        end
        it.sadd(PROCESS_SET, current_probed_processes.to_a) unless current_probed_processes.empty?
      end
    end

    def invalidate_old_processes
      Sidekiq.redis do |it|
        removed_old_processes = remove_old_processes!

        Sidekiq::Queue.instances.each do |queue|
          queue.remove_locks_for! removed_old_processes
        end
      end
    end

    def heartbeat_key(process=Selector.uuid)
      HEARTBEAT_PREFIX + process
    end

    private
    def run_limit_heartbeat(ttl)
      Sidekiq::LimitFetch.redis_retryable do
        add_dynamic_queues
        update_heartbeat ttl
        note_current_probed_processes
        invalidate_old_processes
      end
    end
  end
end

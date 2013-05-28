# MapReduce Worker make two jobs:
#   First, it maps (emits) all data to masters;
#   Second, it reduces data returned form master;
#
module MapReduce
  class Worker

    # Valid options:
    #   * masters - socket addresses of masters, 
    #       default is 'ipc:///dev/shm/master.sock'
    #   * type - connection type:
    #     ** :em - Eventmachine with callbacks (default)
    #     ** :sync - Synchronous type on Fibers
    #
    def initialize(opts = {})
      @master_sockets = opts[:masters] || [::MapReduce::DEFAULT_SOCKET]

      opts[:type] ||= :em
      @socket_class = case opts[:type]
      when :em
        require File.expand_path("../socket/worker_em", __FILE__)
        MapReduce::Socket::WorkerEm
      when :sync
        require File.expand_path("../socket/worker_sync", __FILE__)
        MapReduce::Socket::WorkerSync
      else
        fail "Wrong Connection type. Choose :em or :sync, not #{opts[:type]}"
      end
    end

    # Sends key and value to master through socket.
    # Key can't be nil.
    #
    def emit(key, value, &blk)
      fail "Key can't be nil"  if key.nil?

      sock = pick_socket(key)
      sock.send_request(["map", key, value], &blk)
    end
    alias :map :emit

    # Explicitly stop MAP phase.
    # Master will wait till all workers will send "map_finished" message.
    #
    def map_finished(&blk)
      all = worker_sockets.size
      resp = 0

      worker_sockets.each do |sock|
        sock.send_request(["map_finished"]) do |msg|
          blk.call  if block_given? && (resp+=1) == all
        end
      end
      ["ok"]
    end

    # Reduce operation.
    # Sends request to all masters.
    # If master returns nil it means that he is already empty:
    #  nothing to reduce.
    # Reducing till any socket returns data.
    # If nothing to reduce, we return nil to client.
    #
    def reduce(&blk)
      sock = random_socket
      if sock
        sock.send_request(["reduce"]) do |message|
          key, *values = message
          if key.nil?
            remove_socket(sock)
          else
            blk.call(key, values)
          end
          reduce(&blk)
        end
      else
        blk.call([nil])
      end
    end

    private

    # Connect to each master.
    #
    def worker_sockets
      @worker_sockets ||= begin
        @master_sockets.map do |addr|
          sock = @socket_class.new
          sock.connect addr
          sock
        end
      end
    end

    # Kind of sharding
    #
    def pick_socket(key)
      shard = if worker_sockets.size > 1
        Digest::MD5.hexdigest(key.to_s).to_i(16) % worker_socket.size
      else
        0
      end
      worker_sockets[shard]
    end

    # Take random socket to get reduce message
    #
    def random_socket
      worker_sockets.sample
    end

    # Remove socket when it is empty
    #
    def remove_socket(sock)
      worker_sockets.delete sock
    end
  end
end
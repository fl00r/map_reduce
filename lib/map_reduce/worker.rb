# MapReduce Worker make two jobs:
#   First, it maps (emits) all data to masters;
#   Second, it reduces data returned form master;
# After reducing he is ready to map data again.
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
      @socket_addrs = opts[:masters] || [::MapReduce::DEFAULT_SOCKET]

      @type = opts[:type] ||= :em
      @socket_class = case @type
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

      sock = pick_map_socket(key)
      sock.send_request(["map", key, value], &blk)
    end
    alias :map :emit

    # Explicitly stop MAP phase.
    # Master will wait till all workers will send "map_finished" message.
    #
    def map_finished(&blk)
      all = master_sockets.size
      resp = 0

      master_sockets.each do |sock, h|
        sock.send_request(["map_finished"]) do |msg|
          socket_state(sock, :reduce)
          blk.call(["ok"])  if block_given? && (resp+=1) == all
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
      if @type == :em
        em_reduce(&blk)
      else
        sync_reduce(&blk)
      end
    end

    def sync_reduce(&blk)
      while sock = random_reduce_socket
        key, *values = sock.send_request(["reduce"])
        if key.nil?
          socket_state(sock, :map)
        else
          blk.call(key, values)
        end
      end
    end

    def em_reduce(&blk)
      sock = random_reduce_socket
      if sock
        sock.send_request(["reduce"]) do |message|
          key, *values = message
          if key.nil?
            socket_state(sock, :map)
          else
            blk.call(key, values)
          end

          em_reduce(&blk)
        end
      else
        blk.call([nil])
      end
    end

    private

    # Connect to each master.
    #
    def master_sockets
      @master_sockets ||= begin
        socks = {}
        @socket_addrs.each_with_index do |addr, i|
          sock = @socket_class.new
          sock.connect addr
          socks[sock] = { state: :map, ind: i }
        end
        socks
      end
    end

    # Kind of sharding
    #
    def pick_map_socket(key)
      shard = if master_sockets.size > 1
        Digest::MD5.hexdigest(key.to_s).to_i(16) % master_sockets.size
      else
        0
      end
      master_sockets.keys[shard]
    end

    # Take random socket to get reduce message.
    # Socket should be in :reduce state.
    #
    def random_reduce_socket
      master_sockets.select{ |k,v| v[:state] == :reduce }.keys.sample
    end

    # Change socket's state to :map when it is empty
    #   and to :reduce when mapping is finished
    #
    def socket_state(sock, state)
      master_sockets[sock][:state] = state
    end
  end
end
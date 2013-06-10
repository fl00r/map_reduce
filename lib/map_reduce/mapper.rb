module MapReduce
  class Mapper
    def initialize(opts = {})
      @masters         = opts[:masters] || [::MapReduce::DEFAULT_SOCKET]
      @connection_type = opts[:type]    || :em
      @task_name       = opts[:task]
    end

    def emit(key, value, &blk)
      raise MapReduce::Exceptions::BlankKey, "Key can't be nil"  if key.nil?

      sock = pick_master(key)
      sock.send_request(["map", key, value, @task_name], &blk)
    end
    alias :map :emit

    def wait_for_all(&blk)
      finished = Hash[socket.map{ |s| [s, false] }]
      sockets.each do |sock|
        sock.send_request(["map_finished", @task_name]) do |message|
          finished[sock] = message[0] == "ok"
          if finished.all?{ |k,v| v }
            if block_given?
              blk.call
            else
              return
            end
          else
            after(1) do
              wait_for_all(&blk)
            end
          end
        end
      end
    end

    private

    def after(sec)
      klass = if @connection_type == :sync
        EM::Synchrony
      else
        EM
      end

      klass.add_timer(sec) do
        yield
      end
    end

    def pick_master(key)
      num = Digest::MD5.hexdigest(key.to_s).to_i(16) % sockets.size
      sockets[num]
    end

    def sockets
      @sockets ||= begin
        klass = if @connection_type == :sync
          EM::Protocols::Zmq2::ReqFiber
        else
          EM::Protocols::Zmq2::ReqCb
        end

        @masters.map do |sock|
          s = klass.new
          s.connect(sock)
          s
        end
      end
    end
  end
end
module MapReduce
  class Reducer
    def initialize(opts = {})
      @masters         = opts[:masters] || [::MapReduce::DEFAULT_SOCKET]
      @connection_type = opts[:type]    || :em
      @task            = opts[:task]
    end

    def reduce(&blk)
      if @connection_type == :em
        em_reduce(&blk)
      else
        sync_reduce(&blk)
      end
    end

    def sync_reduce(&blk)
      all = sockets.dup
      while sock = all.sample
        key, *values = sock.send_request(["reduce", @task])
        if key.nil?
          all.delete sock
        else
          blk.call(key, values)
        end
      end
    end

    def em_reduce(all = nil, &blk)
      all ||= sockets.dup
      sock = all.sample
      if sock
        sock.send_request(["reduce", @task]) do |message|
          key, *values = message
          if key.nil?
            all.delete sock
          else
            blk.call(key, values)
          end

          em_reduce(all, &blk)
        end
      else
        blk.call([nil])
      end
    end
    private

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
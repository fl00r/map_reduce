module MapReduce::Socket
  class WorkerSync < EM::Protocols::Zmq2::ReqCb
    alias_method :async_send_request, :send_request
    def send_request(data, &blk)
      fib = Fiber.current
      async_send_request(data) do |message|
        fib.resume(message)
      end
      if block_given?
        blk.call Fiber.yield
      else
        Fiber.yield
      end
    end
  end
end
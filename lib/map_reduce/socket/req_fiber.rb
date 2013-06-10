module EM::Protocols::Zmq2
  class ReqFiber < EM::Protocols::Zmq2::ReqCb
    def send_request(data, &blk)
      fib = Fiber.current
      super(data) do |message|
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
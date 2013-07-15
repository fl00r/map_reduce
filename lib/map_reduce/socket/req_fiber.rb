module EM::Protocols::Zmq2
  class ReqFiber < EM::Protocols::Zmq2::ReqCb
    def send_request(data, &blk)
      fib = Fiber.current
      timer = nil
      request_id = super(data) do |message|
        EM.cancel_timer(timer)
        fib.resume(message)
      end
      if request_id
        timer = EM.add_timer(1) {
          MapReduce.logger.info("TIMEOUT")
          cancel_request(request_id)
        }
      end
      if block_given?
        blk.call Fiber.yield
      else
        Fiber.yield
      end
    end
  end
end
module MapReduce::Socket
  class Master < EM::Protocols::Zmq2::Rep
    def initialize(master)
      @master = master
      super()
    end

    def receive_request(message, envelope)
      @master.recieve_msg(message, envelope)
    end
  end
end
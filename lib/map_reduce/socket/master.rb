# Reply socket.
# Master accepts "map", "map_finished", and "reduce" messages.
# For "map" messages it didn't actually replies,
#   but for "reduce" requests it returns key with grouped values.
#
module MapReduce::Socket
  class Master < EM::Protocols::Zmq2::Rep
    # If worker is ready to reduce data, but we are still in MAP state
    # we will sleep for REDUCE_WAIT seconds till state is not REDUCE
    REDUCE_WAIT = 1

    def initialize(master, workers)
      @master = master
      @workers = workers

      @connections = {}
      @state = :map

      super()
    end

    def receive_request(message, envelope)
      @connections[envelope.first] = false

      type, key, msg = message
      case type 
      when "map"
        map(envelope, key, msg)
      when "map_finished"
        map_finished(envelope)
      when "reduce"
        reduce(envelope)
      else
        MapReduce.logger.error("Wrong message type: #{type}")
      end
    end

    # Send data to log
    # Someone should never MAP data when master already in REDUCE state
    #
    def map(envelope, key, msg)
      if @state == :map
        @master.map(key, msg)
        ok(envelope)
      else
        MapReduce.logger.error("Someone tries to MAP data while state is REDUCE")
        not_ok(envelope, "You can't MAP while we are reducing")
      end
    end

    # When worker stops mapping data, it sends "map_finished" message.
    # When all workers will send "map_finished" message reduce will begin.
    #
    def map_finished(envelope)
      ok(envelope)

      @connections[envelope.first] ||= true
      @workers = @connections.size  if @workers == :auto

      return  unless @connections.all?{ |k,v| v }
      return  unless @connections.size == @workers

      @state = :reduce
      @master.reduce!
    end

    # Wait till all workers stopps sending MAP.
    # After all workers stopped we start REDUCE part of job.
    #
    def reduce(envelope)
      @connections[envelope] ||= true
      if @state == :reduce
        @master.reduce(envelope)
      else
        EM.add_timer(REDUCE_WAIT) do
          reduce(envelope)
        end
      end
    end

    # Simple OK reply
    #
    def ok(envelope)
      send_reply(["ok"], envelope)
    end

    # Simple NOT OK reply
    #
    def not_ok(envelope, error)
      send_reply(["error", error], envelope)
    end
  end
end
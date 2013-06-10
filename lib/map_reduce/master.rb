require File.expand_path("../socket/master", __FILE__)

module MapReduce
  class Master
    def initialize(opts = {})
      @socket_addr = opts[:socket]     || ::MapReduce::DEFAULT_SOCKET
      @log_folder  = opts[:log_folder] || "/tmp/map_reduce"
      @delimiter   = opts[:delimiter]  || "\t"

      @tasks = {}
    end

    def run
      EM.run do
        socket
      end
    end

    def after_map(&blk)
      @after_map = blk
    end

    def after_reduce(&blk)
      @after_reduce = blk
    end

    def recieve_msg(message, envelope)
      mtype = case message[0]
      when "map"
        store_map(message, envelope)
      when "map_finished"
        all_finished?(message, envelope)
      when "reduce"
        send_reduce(message, envelope)
      else
        MapReduce.logger.error("Wrong message type: #{mtype}")
      end
    end

    private

    def store_map(message, envelope)
      status, key, value, task = message
      map_log(task) << "#{key}#{@delimiter}#{value}"
      ok(envelope)
      register(task, envelope, "mapper", status)

      @after_map.call(key, value, task)  if @after_map
    end

    def send_reduce(message, envelope)
      status, task = message

      data = if @tasks.fetch(task, {}).fetch("reducer", {}).fetch(envelope[0], nil) == "reduce"
        reduce_log(task).get_data
      else
        reduce_log(task, true).get_data
      end

      reply(data, envelope)

      if data
        register(task, envelope, "reducer", status)
      else
        register(task, envelope, "reducer", "reduce_finished")
      end

      @after_reduce.call(data[0], data[1], task)  if data && @after_reduce
    end

    def all_finished?(message, envelope)
      status, task = message
      register(task, envelope, "mapper", status)
      if @tasks[task]["mapper"].all?{ |k,v| v == status }
        ok(envelope)
      else
        no(envelope)
      end
    end

    def map_log(task)
      @map_log ||= {}
      @map_log[task] ||= MapReduce::MapLog.new(@log_folder, task)
    end

    def reduce_log(task, force = false)
      @reduce_log ||= {}
      log = @reduce_log[task] ||= MapReduce::ReduceLog.new(map_log(task), @delimiter)
      @reduce_log[task].force  if force
      log
    end

    def ok(envelope)
      reply(["ok"], envelope)
    end

    def np(envelope)
      reply(["not ok"], envelope)
    end

    def reply(resp, envelope)
      socket.send_reply(resp, envelope)
    end

    def register(task, envelope, type, status)
      @tasks[task] ||= {}
      @tasks[task][type] ||= {}
      @tasks[task][type][envelope[0]] = status
    end

    def socket
      @socket ||= begin
        master = self
        sock = MapReduce::Socket::Master.new(self)
        sock.bind(@socket_addr)
        sock
      end
    end
  end
end
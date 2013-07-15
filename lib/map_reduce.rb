require "map_reduce/version"
require "digest/sha1"
require "em-zmq-tp10"
require "logger"

module MapReduce
  DEFAULT_SOCKET = "ipc:///dev/shm/master.sock"

  extend self

  def logger
    @logger ||= begin
      log = Logger.new(STDOUT)
      log.formatter = Logger::Formatter.new
      log
    end
  end
end

require File.expand_path("../map_reduce/exceptions", __FILE__)
require File.expand_path("../map_reduce/socket/req_fiber", __FILE__)
require File.expand_path("../map_reduce/map_log", __FILE__)
require File.expand_path("../map_reduce/reduce_log", __FILE__)
require File.expand_path("../map_reduce/socket/master", __FILE__)
require File.expand_path("../map_reduce/master", __FILE__)
require File.expand_path("../map_reduce/mapper", __FILE__)
require File.expand_path("../map_reduce/reducer", __FILE__)
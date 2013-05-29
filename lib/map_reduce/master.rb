require File.expand_path("../socket/master", __FILE__)

module MapReduce
  class Master
    # How often data will be flushed to disk
    FLUSH_TIMEOUT = 1
    # How many lines should be parsed by one iteration of grouping
    GROUP_LINES = 100
    # How many seconds should we sleep if grouping is going faster then reducing
    GROUP_TIMEOUT = 1
    # How many keys should be stored before timeout happend
    GROUP_MAX = 10_000

    # Valid options:
    #   * socket - socket address to bind
    #       default is 'ipc:///dev/shm/master.sock'
    #   * log_folder - folder to store recieved MAP data
    #       default is '/tmp/mapreduce/'
    #   * workers - count of workers that will emit data.
    #       default is :auto, 
    #       but in small jobs it is better to define in explicitly,
    #       because if one worker will stop before others start
    #       master will decide that map job is done and will start reducing
    #   * delimiter - master log stores data like "key{delimiter}values"
    #       so to prevent collisions you can specify your own uniq delimiter
    #       default is a pipe "|"
    #
    def initialize(opts = {})
      # Socket addr to bind
      @socket_addr = opts[:socket] || ::MapReduce::DEFAULT_SOCKET
      # Folder to write logs
      @log_folder = opts[:log_folder] || "/tmp/mapreduce/"
      # How many MapReduce workers will emit data
      @workers = opts[:workers] || 1
      # Delimiter to store key/value pairs in log
      @delimiter = opts[:delimiter] || "|"

      @log = []
      @data = []
      @workers_envelopes = {}
      @log_filename = File.join(@log_folder, "master-#{Process.pid}.log")
      @sorted_log_filename = File.join(@log_folder, "master-#{Process.pid}_sorted.log")

      FileUtils.mkdir_p(@log_folder)
      FileUtils.touch(@log_filename)
    end

    # Start Eventloop
    #
    def run
      EM.run do
        # Init socket
        master_socket

        # Init flushing timer
        flush
      end
    end

    # Stop Eventloop
    #
    def stop
      EM.stop
    end

    # Store data in log array till flush
    #
    def map(key, message)
      @log << "#{key}#{@delimiter}#{message}"
    end

    # Send data back to worker.
    # Last item in data is last unfinished session,
    #   so till the end of file reading we don't send it
    #
    def reduce(envelope)
      if @data.size >= 2 
        data = @data.shift
        data = data.flatten
        master_socket.send_reply(data, envelope)
      elsif @reduce_stop
        data = @data.shift
        data = data.flatten  if data
        master_socket.send_reply(data, envelope)
      else
        EM.add_timer(1) do
          reduce(envelope)
        end
      end
    end

    # Openning log file for read/write
    #
    def log_file
      @log_file ||= begin
        File.open(@log_filename, "w+")
      end
    end

    # Openning sorted log for reading
    #
    def sorted_log_file
      @sorted_log_file ||= begin
        File.open(@sorted_log_filename, "r")
      end
    end

    # Flushing data to disk once per FLUSH_TIMEOUT seconds
    #
    def flush
      if @log.any?
        log_file << @log*"\n" << "\n"
        log_file.flush
        @log.clear
      end

      EM.add_timer(FLUSH_TIMEOUT) do
        flush
      end
    end

    # Sorting log.
    # Linux sort is the fastest way to sort big file.
    # Deleting original log after sort.
    #
    def sort
      `sort #{@log_filename} -o #{@sorted_log_filename}`
      FileUtils.rm(@log_filename)
      @log_file = nil
    end

    # Start reducing part.
    # First, flushing rest of log to disk.
    # Then sort data.
    # Then start to read/group data
    #
    def reduce!
      flush
      sort

      iter = sorted_log_file.each_line
      group iter
    end

    # Reading sorted data and grouping by key.
    # If queue (@data) is growing faster then workers grad data we pause reading file.
    #
    def group(iter)
      if @data.size >= GROUP_MAX
        EM.add_timer(GROUP_TIMEOUT){ group(iter) }
      else
        GROUP_LINES.times do
          line = iter.next.chomp
          key, msg = line.split(@delimiter)

          last = @data.last
          if last && last[0] == key
            last[1] << msg
          else
            @data << [key, [msg]]
          end
        end

        EM.next_tick{ group(iter) }
      end
    rescue StopIteration => e
      FileUtils.rm(@sorted_log_filename)
      @sorted_log_file = nil
      @reduce_stop = true
    end

    # Initializing and binding socket
    #
    def master_socket
      @master_socket ||= begin
        sock = MapReduce::Socket::Master.new self, @workers
        sock.bind @socket_addr
        sock
      end
    end
  end
end
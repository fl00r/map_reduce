module MapReduce
  class MapLog
    MAX_BUFFER_SIZE = 2 ** 21 # 2 MB

    attr_reader :log_folder

    def initialize(log_folder, task)
      @task = task || "default"
      @log_folder = File.join(log_folder, @task)
      @log = ""
      @log_size = 0
    end

    def <<(str)
      @log_size += str.bytesize
      @log << str << "\n"
      flush  if @log_size >= MAX_BUFFER_SIZE
    end

    def flush
      unless @log.empty?
        log_file << @log
        log_file.flush
        @log.clear
        @log_size = 0
      end
    end

    def reset
      flush
      if @log_file
        map_fn = File.path(@log_file)
        @log_file.close
        reduce_fn = File.join(@log_folder, "reducer.log")
        File.rename(map_fn, reduce_fn)
        @log_file = nil
        reduce_fn
      end
    end

    def log_file
      @log_file ||= begin
        begin
          fn = File.join(@log_folder, "mapper.log")
        end while File.exist?(fn)
        FileUtils.mkdir_p(@log_folder)
        File.open(fn, "a")
      end
    end
  end
end
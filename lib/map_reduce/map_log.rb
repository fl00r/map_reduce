module MapReduce
  class MapLog
    MAX_BUFFER_SIZE = 2 ** 20

    def initialize(log_folder, task)
      @log_folder = log_folder
      @task = task
      @log = ""
      @log_size = 0
    end

    def <<(str)
      @log_size += str.size
      @log << str << "\n"
      flush  if @log_size >= MAX_BUFFER_SIZE
    end

    def flush
      unless @log.empty?
        log_file << @log
        log_file.flush
      end
    end

    def reset
      flush
      if @log_file
        fn = File.path(@log_file)
        @log_file.close
        @log_file = nil
        fn
      end
    end

    def log_file
      @log_file ||= begin
        begin
          fn = File.join(@log_folder, "map_#{@task}_#{Time.now.to_i}_#{rand(1000)}.log")
        end while File.exist?(fn)
        FileUtils.mkdir_p(@log_folder)
        File.open(fn, "a")
      end
    end
  end
end
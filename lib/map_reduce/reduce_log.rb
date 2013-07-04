module MapReduce
  class ReduceLog
    def initialize(map_log, delimiter)
      @map_log = map_log
      @delimiter = delimiter
    end

    def get_data
      if @lines
        current_key = nil
        current_values = []
        while true
          begin
            line = @lines.peek.chomp
            key, values = line.split(@delimiter)
            current_key ||= key

            if current_key != key
              break
            else
              current_values << values
              @lines.next
            end
          rescue StopIteration => e
            @file.close
            FileUtils.rm(File.path(@file))
            @lines = nil
            break
          end
        end
        [current_key, *current_values]
      end
    end

    def force
      unless @lines
        fn = log_file
        if fn
          @file = File.open(fn)
          @lines = @file.each_line
        end
      end
    end

    def log_file
      fn = @map_log.reset
      if fn
        @more = true
        sort(fn)
        fn
      end
    end

    def sort(fn)
      `sort #{fn} -o #{fn}`
    end
  end
end
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
        line = nil
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
            File.unlink(File.path(@file))
            @lines = nil
            break
          rescue => e
            MapReduce.logger.error("#{e.message} for line #{line.inspect}")
            @lines.next
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
      reduce_fn = File.join(@map_log.log_folder, "reducer.log")
      if File.exist? reduce_fn
        reduce_fn
      else
        reduce_fn = @map_log.reset
        if reduce_fn
          sort(reduce_fn)
          reduce_fn
        end
      end
    end

    def sort(fn)
      `sort #{fn} -o #{fn} -k1nr`
    end
  end
end
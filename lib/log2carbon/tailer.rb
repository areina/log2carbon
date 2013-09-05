module Log2Carbon
  module Tailer
    @@lines_processed = 0

    def self.tail(collector, filenames, eof_block, &block)
      @restart = false
      @finish = false
      @lines_processed = 0

      ## convert to array if they only pass a string
      filenames = [filenames] if filenames.class==String

      while !@finish
        files = Hash.new

        filenames.each do |filename|
          files[filename] = File.open(filename,"r")
          collector.last_timestamp_of_log_files[filename]=""
        end

        while !@restart
          file = files[collector.last_timestamp_of_log_files.sort {|a1,a2| a1[1]<=>a2[1]}.first.first]
          begin
            @@lines_processed += 1
            line = file.readline
            block.call line, file.path unless line.nil?
          rescue EOFError
            eof_block.call file.path unless eof_block.nil?
            file.seek(0, File::SEEK_CUR)
            sleep 1.0
          end
        end

        @restart = false
        files.each do |filename, logfile|
          logfile.close
        end
      end
    end

    def self.restart
      @@lines_processed = 0
      @restart = true
    end

    def self.stop
      @@lines_processed = 0
      @finish = true
    end

    def self.lines_processed
      @@lines_processed
    end
  end
end

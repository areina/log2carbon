module Log2Carbon
  module Tailer
    @@lines_processed = 0
    
    def self.tail(filenames, eof_block, &block)
      @restart = false
      @finish = false
      @lines_processed = 0

      ## convert to array if they only pass a string
      filenames = [filenames]  if filenames.class==String
        
      while !@finish
        files = Array.new
        
        filenames.each do |filename|
          files << File.open(filename,"r")
        end
       
        while !@restart
          num_eofs = 0
          files.each do |file|
            begin
              @@lines_processed += 1
              line = file.readline
              block.call line, file.path unless line.nil?
            rescue EOFError
              eof_block.call file.path unless eof_block.nil?
              file.seek(0, File::SEEK_CUR)
              num_eofs += 1
            end
          end
          
          ## this strategy is not very smart if doing a seek is 
          ## very expensive
          sleep 1.0 if num_eofs >= files.size
        end
        
        @restart = false
        files.each do |file|
          file.close
        end
      end
    end  
    
    def self.restart()
      @@lines_processed = 0
      @restart = true
    end
    
    def self.stop()
      @@lines_processed = 0
      @finish = true
    end
    
    def self.lines_processed
      @@lines_processed
    end
  end
end

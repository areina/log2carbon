module Log2Carbon
  module Tailer
    def self.tail(filename, &block)
      @restart = false
      @finish = false
      while !@finish
        f = File.open(filename,"r")
        while !@restart
          begin
            line = f.readline
            block.call line unless line.nil?
          rescue EOFError
            f.seek(0, File::SEEK_CUR)
            sleep 1.0 
          end
        end
        @restart = false
        f.close
      end
    end  
    
    def self.restart()
      @restart = true
    end
    
    def self.finish()
      @finish = true
    end
  end
end
    
#def doing_hup
#  puts "doing HUP"
#end    

#trap('HUP') {
#  puts "doing hup: #{@restart} -> true"
#  @restart = true
#}

#trap('HUP') { doing_hup() }

#Tailer.tail(ARGV[0]) do |line|
#  puts "tailed: #{line}"
#end    
    

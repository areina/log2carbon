module Log2Carbon
  
  module Configuration
    
    def self.check_connection_to_carbon!(server)
      begin
        Socket.getaddrinfo(server[:address],nil)
        if server[:port]=="udp"
          carbon_socket = UDPSocket.new
          carbon_socket.close
        elsif server[:port]=="tcp"
          carbon_socket = TCPSocket.new(server[:address], server[:port])
          carbon_socket.close
        else
          raise "Server port \"#{server[:port]} is not a valid protocol, must be TCP or UDP"
        end
      rescue Exception => e
        raise "Could not connect to the carbon server: #{server}"
      end
    end
  
    def self.check_file_exists(log)      
      raise Exception.new("The log file \"log\" to be analyzed does not exist") unless File.exists?(log)
    end
    
    def self.check_parser(parser)
      
    end
  
    def self.load(conf_file)
      begin
        conf = {}
        File.open(conf_file).each do |line|
          if line.match(/^#/).nil? || line.empty?
            ## not a comment
            
            line = line.rstrip.lstrip.gsub("\n","")
            items = line.split(" ")
            raise "Parsing error in conf file in line \"#{line}\"" if items.size<2
            
            lab = items.first.downcase 

            if lab=="carbon_server"
              address, port, protocol = items[2..4]
              conf[:carbon_server] = {:address => address, :port => port.downcase!, :protocol => protocol}
              check_connection_to_carbon(conf[:carbon_server]) 
            elsif lab=="parser_dir"
              conf[:parser_dir] = items[2]
              check_file_exists(conf[:parser_dir])
              Dir["#{conf[:parser_dir]}/**.rb"].each do |parser|
                require parser
              end
            elsif lab=="log2carbon_logfile"
              conf[:logfile] = items[2]
            elsif lab=="log_entry"
              raise "parser_dir must be defined before any log_entry" if conf[:parser_dir].nil?
              conf[:logs] ||= Array.new
              logs, parser = Dir[items[2]], items[3]
              check_parser(parser)
              logs.each do |log|
                check_file_exists(log)
                conf[:logs] << {:file => log, :parser => parser}
              end
              
            end
              
            end
                
              
            

            ## the first line that is not a comment is the address to carbon,
            ## the rest of the lines will be carbon
            first_line = false
            address, port, protocol = line.split(" ")

              raise "The conf line \"#{line}\" is not a valid carbon server" if address.nil? || port.nil? || protocol.nil? 

              conf[:carbon] = [{:address => address, :port => port.downcase!, :protocol => protocol}]
              check_connection_to_carbon!(@conf[:carbon].first)
            else

            end  
          end
        end
        return conf
      rescue Exception => e
        puts "CRITICAL: Error loading the configuration file #{conf_file}"
        puts e.inspect
        raise e
      end  
    end
    
  end
end
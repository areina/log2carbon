
module Log2Carbon
  
  module Configuration
    def check_connection_to_carbon!(server)
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
  
    def load_conf(conf_file)
      begin
        conf = {}
        first_line = true
        File.open(conf_file).each do |line|
          if line.match(/^#/).nil? || line.empty?
            ## not a comment
            line = line.rstrip.lstrip.gsub("\n","")

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
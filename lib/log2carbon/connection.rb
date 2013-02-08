module Log2Carbon
  class Connection
    def initialize(host, port, protocol)
      @host = host
      @port = port
      @protocol = protocol
    end
    
    def send(data)
      ## no need to keep the connection alive since it's one per minute max
      begin 
        bytes = 0
        if @protocol==:udp
          carbon_socket = UDPSocket.new
          carbon_socket.connect(@host, @port)
          data.each do |item|
            bytes += item.size 
            carbon_socket.send(item,0)
          end
          carbon_socket.close
        else
          carbon_socket = TCPSocket.new(@host, @port)
          data.each do |item|
            bytes += item.size 
            carbon_socket.write(item)
          end
          carbon_socket.close
        end
        ##Analyzer.logger.info("Success on sending data to carbon, sent #{bytes} bytes")
      rescue Exception => e
        Analyzer.logger.error("Failure to send data to carbon: #{e.inspect}")
      end
    end
  end
end
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
        if protocol==:udp
          carbon_socket = UDPSocket.new
          carbon_socket.send(data,0,@host, @port)
          carbon_socket.close
        else
          carbon_socket = TCPSocket.new(@host, @port)
          carbon_socket.send(data)
          carbon_socket.close
        end
        Analyzer.logger.info("Success on sending data to carbon, sent #{data.size} bytes")
      rescue Exception => e
        Analyzer.logger.error("Failure to send data to carbon: #{e.inspect}")
      end
    end
  end
end
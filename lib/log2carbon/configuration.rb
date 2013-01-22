module Log2Carbon
  
  module Configuration
    
    def self.check_connection_to_carbon!(server)
      begin
        Socket.getaddrinfo(server[:address],nil)
        if server[:protocol]==:udp
          carbon_socket = UDPSocket.new
          carbon_socket.connection(server[:address], server[:port])
          carbon_socket.close
        elsif server[:protocol]==:tcp
          carbon_socket = TCPSocket.new(server[:address], server[:port])
          carbon_socket.close
        else
          raise "Server port \"#{server[:port]} is not a valid protocol, must be TCP or UDP"
        end
      rescue Exception => e
        raise "Could not connect to the carbon server: #{server}, #{e.message}"
      end
    end
  
    def self.check_file_exists!(log)      
      raise "The log file \"#{log}\" to be analyzed does not exist" unless File.exists?(log)
    end
    
    def self.check_parser!(parser)
      begin 
        klass = Module.const_get(parser)
        raise "The parser \"parser\" does not have the process method defined" if klass.methods.find_index(:process).nil?
      rescue Exception => e
        raise "The parser \"parser\" can not be loaded"
      end
    end
    
    def self.check_config!(config)
      [:conf_file, :carbon_server, :parser_dir, :log_file, :logs].each do |lab|
        lab_s = lab
        lab_s = "log_entry" if lab==:logs
        raise "The configuration \"#{config.inspect}\" is missing the mandatory field #{lab_s}" if config[lab].nil?
      end
    end
  
    def self.load(conf_file)
      begin
        conf = {}
        File.open(conf_file).each do |line|
          conf[:conf_file] = conf_file
          line = line.rstrip.lstrip.gsub("\n","")
          if line.match(/^#/).nil? && !line.empty? 
            items = line.split(" ")
            raise "Parsing error in conf file in line \"#{line}\"" if items.size<2
            
            lab = items.first.downcase.to_sym 

            if lab==:carbon_server
              address, port, protocol = items[1..3]
              raise "carbon_server is not defined, must be: carbon_server HOST/IP PORT PROTOCOL" if address.nil? || port.nil? || protocol.nil?
              conf[:carbon_server] = {:address => address, :port => port.to_i, :protocol => protocol.downcase!.to_sym}
              check_connection_to_carbon!(conf[:carbon_server]) 
            elsif lab==:parser_dir
              conf[:parser_dir] = items[1]
              check_file_exists!(conf[:parser_dir])
              Dir["#{conf[:parser_dir]}/**.rb"].each do |parser|
                require parser
              end
            elsif lab==:log_file
              conf[:log_file] = items[1]
            elsif lab==:log_entry
              raise "parser_dir must be defined before any log_entry" if conf[:parser_dir].nil?
              conf[:logs] ||= Array.new
              logs, parser = Dir[items[1]], items[2]
              check_parser!(parser)
              logs.each do |log|
                check_file_exists!(log)
                conf[:logs] << {:file => log, :parser => parser}
              end
            elsif lab==:resolution
              conf[:resolution] = items[1].to_sym
              raise "resolution must be second, minute, hour, day" unless Collector::RESOLUTION.member?(conf[:resolution])
            elsif lab==:polling_time
              conf[:polling_time] = items[1].to_i
              raise "polling time must be at least 1 second" if conf[:polling_time]<=0
            elsif lab==:flush_timeout
              conf[:flush_timeout] = items[1].to_i
              raise "flush timeout must be at least 1 second" if conf[:flush_timeout]<=0
            end  
          end
        end
        check_config!(conf)
        return conf
      rescue Exception => e
        raise "Error loading the configuration file #{conf_file}. Message: #{e.message}"
      end  
    end
  end
end
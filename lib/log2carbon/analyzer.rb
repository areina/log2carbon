module Log2Carbon
  class Analyzer
    
    @@logger = nil
    @@config = nil
    @@connection = nil
    
    def initialize(config)
      @@config = config
      
      trap('HUP') {
        reload()
      }
      
      trap('EXIT') {
        stop()
      }
      
      trap('QUIT') {
        stop()
      }
      
      logger
      connection
      work
    end
    
    def self.work(config)
      new(config).work
    end
    
    def work()
      
      @collector = Log2Carbon::Collector.new(config[:resolution],config[:polling_time],config[:flush_timeout])
      parser_classes = Hash.new
      parser_by_log = Hash.new
      logs = Array.new
      
      config[:logs].each do |item|
        parser_by_log[item[:file]] = item[:parser]
        logs << item[:file]
        ## to avoid doing the Module on each log entry, memoizing
        parser_classes[item[:parser]] = Module.const_get(item[:parser])  
      end
      
      logs.each do |log_file|
        logger.info("Starting to read #{log_file}")
      end
      cont ||= 0
      
      eof_hook = Proc.new { |filename| collector.last_timestamp_of_file_due_to_eof(filename); collector.flush_events_if_needed() }
      
      Log2Carbon::Tailer.tail(logs, eof_hook) do |line, log_file|
        ## call the appropiate parser
        klass = parser_classes[parser_by_log[log_file]]
        begin
          cont+=1
          puts "cont #{cont}" if (cont%1000)==1
          klass.process(collector, log_file, line)
          collector.flush_events_if_needed()
        rescue Exception => e
          logger.error("Error parsing the log \"#{log_file}\" on line \"#{line}\" with parser \"#{parser_by_log[log_file]}\". Trace: #{e}")
          raise e
        end
      end
      
    end
    
    def stop()
      begin
        logger.info("Attempting stop")
        Log2Carbon::Tailer.stop()
        collector.stop()
        logger.info("Successful stop")
      rescue Exception => e
        logger.error("Failed to stop #{e.inspect}")
      end
    end
    
    def reload() 
      begin
        logger.info("Attempting reload")
        new_config = Log2Carbon::Configuration.load(config[:conf_file])
        @@config = new_config
        
        ## must stop the tailer of the current logs
        Log2Carbon::Tailer.stop()
        collector.stop()
          
        work()
        logger.info("Successful reload")
      
      rescue Exception => e
        logger.error("Failed to reload #{e.inspect}")
      end
      
    end
    
    def parser_class(name)
      @parsers_classes[name]
    end
    
    def logger
      Analyzer.logger
    end
    
    def self.logger
      if @@logger.nil?
        @@logger = Logger.new(config[:log_file])
        @@logger.formatter = proc { |severity, datetime, progname, msg|
          "#{severity} #{datetime.getutc.strftime("[%d/%b/%Y %H:%M:%S %Z]")} #{msg}\n"
        }
      end
      @@logger
    end
    
    def self.config
      @@config
    end
    
    def config
      Analyzer.config
    end
    
    def self.connection
      server = config[:carbon_server]
      @@connection ||= Log2Carbon::Connection.new(server[:address],server[:port],server[:protocol])
    end
    
    def connection
      Analyzer.connection
    end
    
    def collector
      @collector
    end
    
  end
end
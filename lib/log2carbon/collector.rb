
module Log2Carbon

  class Collector
    
    RESOLUTION = [:second, :hour, :minute, :day]
    @@memoized_timestamps = Hash.new
    
    def initialize(resolution, polling_time, flush_timeout)
      @metrics = Hash.new
      @last_timestamp_analyzed_by_log = Hash.new
      
      @resolution = resolution.to_sym
      @flush_timeout = flush_timeout
      @polling_time = polling_time
      @current_clock = Time.now.getutc
      @check_for_flushing = false
      
      @clock = Thread.new {
        while true        
          sleep(@polling_time)  
          @current_clock = Time.now.getutc
          @check_for_flushing = true 
        end
      }.run
    end
    
    def stop()
      @clock.exit()
      flush_events(:force => true)
    end
   
    def time_bucket_elapsed(bucket_time)
      @last_timestamp_analyzed_by_log.each do |log_file, bucket_time_by_log|
        return false if (bucket_time_by_log <= bucket_time)
      end
      return true
    end
   
    def flush_events_if_needed()
      if @check_for_flushing==true
        ## not thread safe, flush_events cannot be called concurrently safely
        flush_events()
        @check_for_flushing = false 
      end
    end
   
    def flush_events(options = {})
      now = Time.now.getutc
      puts "flush_events :"
     
      @metrics.each do |bucket_time, data|    
        puts "flush_events : #{time_bucket_elapsed(bucket_time)}"  
        if options[:force]==true || time_bucket_elapsed(bucket_time)
          ## the bucket should be flushed to carbon
          flush_to_carbon(bucket_time)            
          @metrics.delete(bucket_time)
        end
      end
      
      @check_for_flushing = false  
    end
  
    def get_current_time_bucket(log_timestamp_str)
      
      if @@memoized_timestamps[log_timestamp_str].nil?        
        log_time = DateTime.parse(log_timestamp_str)
      
        if @resolution==:second
          s = "%02d:%02d:%02d" % [log_time.hour, log_time.minute, log_time.second]
        elsif @resolution==:minute
          s = "%02d:%02d:%02d" % [log_time.hour, log_time.minute, 0]
        elsif @resolution==:hour
          s = "%02d:%02d:%02d" % [log_time.hour, 0, 0]
        elsif @resolution==:day
          s = "00:00:00"
        else
          s = "00:00:00"
        end
      
        bucket_time = "%04d/%02d/%02d %s UTC" % [log_time.year, log_time.month, log_time.day, s]
        
        @@memoized_timestamps = Hash.new if @@memoized_timestamps.size > 1000
        @@memoized_timestamps[log_timestamp_str] = bucket_time
      end
      
      @@memoized_timestamps[log_timestamp_str]
    end
    
    def flush_to_carbon(time_bucket)
      entries = Array.new
      timestamp = Time.parse(time_bucket).to_i

      [time_bucket].each do |metric, data|
        if data[:type]==:last
          entries << "#{metric}.last #{data[:value]}"
        elsif data[:type]==:count
          entries << "#{metric}.count #{data[:value]}"
        elsif data[:type]==:set
          sv = data[:value].sort

          sum = 0.0
          sv.each { |b| sum += b }

          entries << "#{metric}.avg #{(sum/sv.size.to_f).round(3)}"
          entries << "#{metric}.min #{sv.first.round(3)}"
          entries << "#{metric}.max #{sv.last.round(3)}"
          entries << "#{metric}.50 #{(sv[(sv.size*0.50).to_i]).round(3)}"
          entries << "#{metric}.90 #{(sv[(sv.size*0.90).to_i]).round(3)}"
          entries << "#{metric}.95 #{(sv[(sv.size*0.95).to_i]).round(3)}"
          entries << "#{metric}.99 #{(sv[(sv.size*0.99).to_i]).round(3)}"
        end
      end

      data = []
      entries.each do |item|
        data << "#{item} #{timestamp}\r\n"
      end
        
      Analyzer.logger.info("Flushing to carbon bucket #{time_bucket} with #{entries.size} entries. #{Tailer.lines_processed} total lines processed")      
      Analyzer.connection.send(data) if entries.size>0
    end
    
    def last_timestamp_of_file_due_to_eof(log_file)
      @last_timestamp_analyzed_by_log[log_file] = get_current_time_bucket(Time.now.getutc.to_s)
    end
  
    ## ----------------------------------------
    ## ----------------------------------------
    ## operations to use on the parsers
    
    def incr(log_file, time_str, metric, value)
      time_bucket = get_current_time_bucket(time_str)
      @last_timestamp_analyzed_by_log[log_file] = time_bucket
      @metrics[time_bucket] ||= Hash.new      
      @metrics[time_bucket][metric] ||= {:type => :count, :value => 0} 
      @metrics[time_bucket][metric][:value] += value
    end

    def add(log_file, time_str, metric, value)
      time_bucket = get_current_time_bucket(time_str)
      @last_timestamp_analyzed_by_log[log_file] = time_bucket
      @metrics[time_bucket] ||= Hash.new
      @metrics[time_bucket][metric] ||= {:type => :set, :value => Array.new} 
      @metrics[time_bucket][metric][:value] << value
    end
    
    def last(log_file, time_str, metric, value)
      time_bucket = get_current_time_bucket(time_str)
      @last_timestamp_analyzed_by_log[log_file] = time_bucket
      @metrics[time_bucket] ||= Hash.new
      @metrics[time_bucket][metric] ||= {:type => :last, :value => 0} 
      @metrics[time_bucket][metric][:value] = value
    end
        
  end
end


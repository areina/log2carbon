
module Log2Carbon

  class Collector
  
    def initialize(carbon_servers, resolution, polling_time, flush_timeout)
      @hash = Hash.new
      @time = Hash.new
      @buckets = Hash.new
    
      @resolution = resolution.to_sym
      @flush_timeout = flush_timeout
      @polling_time = polling_time
      @current_clock = Time.now.getutc
      
      @carbon_servers_list = carbon_servers
    
      ##@carbon_socket = UDPSocket.new
      @carbon_socket = TCPSocket.new("23.22.57.143", 2003)
     
    
      @t = Thread.new {
        while true        
          sleep(@polling_time)  
          @current_clock = Time.now.getutc
          @check_for_flushing = true 
        end
      }.run
    
    end
    
    def flush(time_bucket)
      puts "flushing #{time_bucket} #{@hash.size}"
      puts @hash[time_bucket].inspect
      puts "-------"
    end
  
    def flush_events
      now = Time.now.getutc
    
      @buckets.each do |bucket, values|
        ##puts "checking buckets: #{bucket} #{now} #{values[:last_write]}"
        if (now - values[:last_write]) > @flush_timeout
          ## the bucket should be flushed
          flush_to_carbon(bucket)
        
          values[:timestamps].each do |log_timestamps_str|
            @time[@resolution].delete(log_timestamps_str)
          end
        
          @buckets.delete(bucket)
          @hash.delete(bucket)
        end
      end
      
      @check_for_flushing = false  
    end
  
  def get_current_time_bucket(log_timestamp_str)
    @time[@resolution] ||= Hash.new

    if !@time[@resolution][log_timestamp_str]
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
      
      @time[@resolution][log_timestamp_str] = Hash.new
      @time[@resolution][log_timestamp_str][:log_time] = log_time
      bucket_time = "%04d/%02d/%02d %s" % [log_time.year, log_time.month, log_time.day, s]
      @time[@resolution][log_timestamp_str][:bucket_time] = bucket_time
      
      if !@buckets[bucket_time]
        @buckets[bucket_time] = Hash.new
        @buckets[bucket_time][:last_write] = @current_clock.clone
        @buckets[bucket_time][:timestamps] = Array.new
      end
      @buckets[bucket_time][:timestamps] << log_timestamp_str
    end

    flush_events if @check_for_flushing
      

    return @time[@resolution][log_timestamp_str][:bucket_time]
  end
  
  def incr(time_str, metric, value)
    time_bucket = get_current_time_bucket(time_str)
    
    if !@hash[time_bucket]
      @hash[time_bucket] = Hash.new
    end 
    @hash[time_bucket][metric] ||= 0
    @hash[time_bucket][metric] = @hash[time_bucket][metric] + value
    @buckets[time_bucket][:last_write] = @current_clock.clone
  end

  def add(time_str, metric, value)
    time_bucket = get_current_time_bucket(time_str)
    if !@hash[time_bucket]
      @hash[time_bucket] = Hash.new
    end
    @hash[time_bucket][metric] ||= Array.new
    @hash[time_bucket][metric] << value
    @buckets[time_bucket][:last_write] = @current_clock.clone
  end
  
  def flush_to_carbon(time_bucket)
    
    entries = Array.new
    
    timestamp = Time.parse(time_bucket).to_i
    
    @hash[time_bucket].each do |metric, value|
      
      klass = value.class
      if klass==Fixnum
        entries << "#{metric} #{value}"
      else
        sv = value.sort
  
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
    
    
    entries.each do |item|
      x = "backend.testing.#{item} #{timestamp}\r\n"
      ## @carbon_socket.puts x
    end
    
    ##puts "Sent bucket #{time_bucket}: #{@hash.size}"
       
  end
    
end 

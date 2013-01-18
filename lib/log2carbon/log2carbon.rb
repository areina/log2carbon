#! /usr/bin/env ruby
require 'daemons'
require 'date'
require 'socket'


module Log2carbon
  
  class Parser
    def self.process(stats, file, line)
      throw Exception("you must overwrite the method self.process to process the log entry!")
    end
  end
  
  
 
  
  
  
  
end


def startup(path)
  @stats = Stats.new(:minute, 1.0, 10.0)
  @shutdown = false
  @path = path
  
  trap('TERM') { shutdown }
  trap('INT')  { shutdown }
end

def shutdown
  puts "GOING TO STOP!!!!!!!!"
  @shutdown = true
end

def hostname
  "localhost"
end

def to_s
  @to_s ||= "#{hostname}:#{Process.pid}}"
end

def run
  puts ">>>>> #{@path}"
  
  File::Tail::Logfile.open(@path, :break_if_eof => false) do |log|
    begin   
      log.tail do |line| 
        process(line)
      end
    
    rescue File::Tail::BreakException => e
      puts "Exception => #{e}"
    end
  end
end

def process(line)

  ##81.19.63.50 - - [27/Dec/2012 09:50:42] "POST /transactions.xml?provider_key=130fbe1bbf5417ce4f04471a109a7b00 HTTP/1.0"
  ##216.74.40.14 - - [27/Dec/2012 09:50:42] "POST /transactions.xml?provider_key=viadeo-043350074c449e5a369db63f00f7f4bb HTTP/1.0" 202 - 0.0070
  #puts line 
  
  line.gsub!('"','')
  ip, ref, ref2, date, time, verb, path, protocol, response_code, bytes_out, request_time = line.split(" ")
  path, qs = path.split("?")
  
  metric = "#{verb}/#{path}".gsub("/","_")
  
  @stats.incr("#{date} #{time}","#{metric}.hits",1)
  @stats.incr("#{date} #{time}","#{metric}.responses.#{response_code}",1)
  @stats.add("#{date} #{time}","#{metric}.time",request_time.to_f)
    
end

## ------------------------------

class Stats
  
  def initialize(resolution, polling_time, flush_timeout)
    @hash = Hash.new
    @time = Hash.new
    @buckets = Hash.new
    
    @resolution = resolution.to_sym
    @flush_timeout = flush_timeout
    @polling_time = polling_time
    @current_clock = Time.now.getutc
    
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

## -----------

class Log2Carbon
  @conf = {}
  
  def initialize()
    
  end
  
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
      first_line = true
      File.open(conf_file).each do |line|
        if line.match(/^#/).nil? || line.empty?
          ## not a comment
          line = line.rstrip.lstrip.gsub("\n","")
          if first_line
            ## the first line that is not a comment is the address to carbon,
            ## the rest of the lines will be carbon
            first_line = false
            address, port, protocol = line.split(" ")
            
            raise "The conf line \"#{line}\" is not a valid carbon server" if address.nil? || port.nil? || protocol.nil? 
    
            @conf[:carbon] = [{:address => address, :port => port.downcase!, :protocol => protocol}]
            check_connection_to_carbon!(@conf[:carbon].first)
          else
            
          end  
        end
      end
      return true
    rescue Exception => ex
      puts "CRITICAL: Error loading the configuration file #{conf_file}"
      puts e
      return false
    end  
  end
  
  
end

Daemons.run_proc('log2carbon', :multiple => true,
                                          :dir_mode => :normal,
                                          :dir      => '/var/run/') do
  
  l2c = Log2Carbon.new
  
  if l2c.load_conf(ARGV[1])
    startup()
    run
  end
  
end

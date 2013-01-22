class Parser3scaleBackend < Log2Carbon::Parser
  def self.process(collector, file, line)

    ##81.19.63.50 - - [27/Dec/2012 09:50:42] "POST /transactions.xml?provider_key=130fbe1bbf5417ce4f04471a109a7b00 HTTP/1.0"
    ##216.74.40.14 - - [27/Dec/2012 09:50:42] "POST /transactions.xml?provider_key=viadeo-043350074c449e5a369db63f00f7f4bb HTTP/1.0" 202 - 0.0070
    #puts line 
  
    line.gsub!('"','')
    ip, ref, ref2, date, time, verb, path, protocol, response_code, bytes_out, request_time = line.split(" ")
    path, qs = path.split("?")
  
    metric = "#{verb}/#{path}".gsub("/","_")
  
    collector.incr("#{date} #{time}","#{metric}.hits",1)
    collector.incr("#{date} #{time}","#{metric}.responses.#{response_code}",1)
    collector.add("#{date} #{time}","#{metric}.time",request_time.to_f)
      
  end
end

module Log2Carbon
  class Parser
    def self.process(stats, file, line)
      throw Exception.new("you must overwrite the method self.process to process the log entry!")
    end
  end
end
module Log2Carbon
  class Parser
    def self.process(stats, file, line)
      raise("you must overwrite the method self.process to process the log entry!")
    end
  end
end

module Polars
  module Utils
    def self.issue_deprecation_warning(message)
      warn message
    end

    def self.deprecated(message)
      issue_deprecation_warning(message)
    end
  end
end

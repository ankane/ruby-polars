module Polars
  module Utils
    def self.issue_unstable_warning(message = nil)
      warnings_enabled = ENV.fetch("POLARS_WARN_UNSTABLE", 0).to_i != 0
      if !warnings_enabled
        return
      end

      if message.nil?
        message = "this functionality is considered unstable."
      end
      message += (
        " It may be changed at any point without it being considered a breaking change."
      )

      warn message
    end
  end
end

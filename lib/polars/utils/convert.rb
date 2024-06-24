module Polars
  module Utils
    def self.parse_as_duration_string(td)
      if td.nil? || td.is_a?(::String)
        return td
      end
      _timedelta_to_duration_string(td)
    end

    def self._timedelta_to_pl_duration(td)
      td
    end

    def self.negate_duration_string(duration)
      if duration.start_with?("-")
        duration[1..]
      else
        "-#{duration}"
      end
    end
  end
end

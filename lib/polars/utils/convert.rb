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

    def self.date_to_int(d)
      dt = d.to_datetime.to_time
      dt.to_i / (3600 * 24)
    end

    def self.datetime_to_int(dt, time_unit)
      dt = dt.to_datetime.to_time
      if time_unit == "ns"
        nanos = dt.nsec
        dt.to_i * 1_000_000_000 + nanos
      elsif time_unit == "us"
        micros = dt.usec
        dt.to_i * 1_000_000 + micros
      elsif time_unit == "ms"
        millis = dt.usec / 1000
        dt.to_i * 1_000 + millis
      elsif time_unit.nil?
        # Ruby has ns precision
        nanos = dt.nsec
        dt.to_i * 1_000_000_000 + nanos
      else
        raise ArgumentError, "time_unit must be one of {{'ns', 'us', 'ms'}}, got #{tu}"
      end
    end
  end
end

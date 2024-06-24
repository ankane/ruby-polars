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
      dt.to_i / SECONDS_PER_DAY
    end

    def self.datetime_to_int(dt, time_unit)
      dt = dt.to_datetime.to_time
      if time_unit == "ns"
        nanos = dt.nsec
        dt.to_i * NS_PER_SECOND + nanos
      elsif time_unit == "us"
        micros = dt.usec
        dt.to_i * US_PER_SECOND + micros
      elsif time_unit == "ms"
        millis = dt.usec / 1000
        dt.to_i * MS_PER_SECOND + millis
      elsif time_unit.nil?
        # Ruby has ns precision
        nanos = dt.nsec
        dt.to_i * NS_PER_SECOND + nanos
      else
        raise ArgumentError, "time_unit must be one of {{'ns', 'us', 'ms'}}, got #{tu}"
      end
    end

    def self._to_ruby_date(value)
      # days to seconds
      # important to create from utc. Not doing this leads
      # to inconsistencies dependent on the timezone you are in.
      ::Time.at(value * 86400).utc.to_date
    end

    def self._to_ruby_time(value)
      if value == 0
        ::Time.utc(2000, 1, 1)
      else
        seconds, nanoseconds = value.divmod(1_000_000_000)
        minutes, seconds = seconds.divmod(60)
        hours, minutes = minutes.divmod(60)
        ::Time.utc(2000, 1, 1, hours, minutes, seconds, nanoseconds / 1000.0)
      end
    end

    def self._to_ruby_datetime(value, time_unit = "ns", time_zone = nil)
      if time_zone.nil? || time_zone == ""
        if time_unit == "ns"
          ::Time.at(value / 1000000000, value % 1000000000, :nsec).utc
        elsif time_unit == "us"
          ::Time.at(value / 1000000, value % 1000000, :usec).utc
        elsif time_unit == "ms"
          ::Time.at(value / 1000, value % 1000, :millisecond).utc
        else
          raise ArgumentError, "time_unit must be one of {{'ns', 'us', 'ms'}}, got #{time_unit}"
        end
      else
        raise Todo
      end
    end

    def self._to_ruby_duration(value, time_unit = "ns")
      if time_unit == "ns"
        value / 1e9
      elsif time_unit == "us"
        value / 1e6
      elsif time_unit == "ms"
        value / 1e3
      else
        raise ArgumentError, "time_unit must be one of {{'ns', 'us', 'ms'}}, got #{time_unit}"
      end
    end

    def self._to_ruby_decimal(digits, scale)
      BigDecimal("#{digits}e#{scale}")
    end
  end
end

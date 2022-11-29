module Polars
  # Series.dt namespace.
  class DateTimeNameSpace
    include ExprDispatch

    self._accessor = "dt"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # def [](item)
    # end

    # Return minimum as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.min
    #   # => 2001-01-01 00:00:00 UTC
    def min
      Utils.wrap_s(_s).min
    end

    # Return maximum as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.max
    #   # => 2001-01-03 00:00:00 UTC
    def max
      Utils.wrap_s(_s).max
    end

    # Return median as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.median
    #   # => 2001-01-02 00:00:00 UTC
    def median
      s = Utils.wrap_s(_s)
      out = s.median.to_i
      Utils._to_ruby_datetime(out, s.dtype, tu: s.time_unit)
    end

    # Return mean as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.date_range(DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.mean
    #   # => 2001-01-02 00:00:00 UTC
    def mean
      s = Utils.wrap_s(_s)
      out = s.mean.to_i
      Utils._to_ruby_datetime(out, s.dtype, tu: s.time_unit)
    end

    # Format Date/datetime with a formatting rule.
    #
    # See [chrono strftime/strptime](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 4)
    #   date = Polars.date_range(start, stop, "1d")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   #         2001-01-04 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.strftime("%Y-%m-%d")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [str]
    #   # [
    #   #         "2001-01-01"
    #   #         "2001-01-02"
    #   #         "2001-01-03"
    #   #         "2001-01-04"
    #   # ]
    def strftime(fmt)
      super
    end

    # Extract the year from the underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number in the calendar date.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2002, 1, 1)
    #   date = Polars.date_range(start, stop, "1y")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2002-01-01 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.year
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [i32]
    #   # [
    #   #         2001
    #   #         2002
    #   # ]
    def year
      super
    end

    # Extract ISO year from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number according to the ISO standard.
    # This may not correspond with the calendar year.
    #
    # @return [Series]
    #
    # @example
    #   dt = DateTime.new(2022, 1, 1, 7, 8, 40)
    #   Polars::Series.new([dt]).dt.iso_year
    #   # =>
    #   # shape: (1,)
    #   # Series: '' [i32]
    #   # [
    #   #         2021
    #   # ]
    def iso_year
      super
    end

    # Extract quarter from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the quarter ranging from 1 to 4.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 4, 1)
    #   date = Polars.date_range(start, stop, "1mo")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-02-01 00:00:00
    #   #         2001-03-01 00:00:00
    #   #         2001-04-01 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.quarter
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         1
    #   #         1
    #   #         1
    #   #         2
    #   # ]
    def quarter
      super
    end

    # Extract the month from the underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the month number starting from 1.
    # The return value ranges from 1 to 12.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 4, 1)
    #   date = Polars.date_range(start, stop, "1mo")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-02-01 00:00:00
    #   #         2001-03-01 00:00:00
    #   #         2001-04-01 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.month
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   # ]
    def month
      super
    end

    # Extract the week from the underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the ISO week number starting from 1.
    # The return value ranges from 1 to 53. (The last week of year differs by years.)
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 4, 1)
    #   date = Polars.date_range(start, stop, "1mo")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-02-01 00:00:00
    #   #         2001-03-01 00:00:00
    #   #         2001-04-01 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.week
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         1
    #   #         5
    #   #         9
    #   #         13
    #   # ]
    def week
      super
    end

    # Extract the week day from the underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the weekday number where monday = 0 and sunday = 6
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 7)
    #   date = Polars.date_range(start, stop, "1d")
    #   # =>
    #   # shape: (7,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   #         2001-01-04 00:00:00
    #   #         2001-01-05 00:00:00
    #   #         2001-01-06 00:00:00
    #   #         2001-01-07 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.weekday
    #   # =>
    #   # shape: (7,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   #         5
    #   #         6
    #   # ]
    def weekday
      super
    end

    # Extract the day from the underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the day of month starting from 1.
    # The return value ranges from 1 to 31. (The last day of month differs by months.)
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 9)
    #   date = Polars.date_range(start, stop, "2d")
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-03 00:00:00
    #   #         2001-01-05 00:00:00
    #   #         2001-01-07 00:00:00
    #   #         2001-01-09 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.day
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [u32]
    #   # [
    #   #         1
    #   #         3
    #   #         5
    #   #         7
    #   #         9
    #   # ]
    def day
      super
    end

    # Extract ordinal day from underlying date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the day of year starting from 1.
    # The return value ranges from 1 to 366. (The last day of year differs by years.)
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 3, 1)
    #   date = Polars.date_range(start, stop, "1mo")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-02-01 00:00:00
    #   #         2001-03-01 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.ordinal_day
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [u32]
    #   # [
    #   #         1
    #   #         32
    #   #         60
    #   # ]
    def ordinal_day
      super
    end

    # Extract the hour from the underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # Returns the hour number from 0 to 23.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 3)
    #   date = Polars.date_range(start, stop, "1h")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 01:00:00
    #   #         2001-01-01 02:00:00
    #   #         2001-01-01 03:00:00
    #   # ]
    #
    # @example
    #   date.dt.hour
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   #         3
    #   # ]
    def hour
      super
    end

    # Extract the minutes from the underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # Returns the minute number from 0 to 59.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 4, 0)
    #   date = Polars.date_range(start, stop, "2m")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:02:00
    #   #         2001-01-01 00:04:00
    #   # ]
    #
    # @example
    #   date.dt.minute
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         2
    #   #         4
    #   # ]
    def minute
      super
    end

    # Extract seconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # Returns the integer second number from 0 to 59, or a floating
    # point number from 0 < 60 if `fractional: true` that includes
    # any milli/micro/nanosecond component.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 0, 4)
    #   date = Polars.date_range(start, stop, "500ms")
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00.500
    #   #         2001-01-01 00:00:01
    #   #         2001-01-01 00:00:01.500
    #   #         2001-01-01 00:00:02
    #   #         2001-01-01 00:00:02.500
    #   #         2001-01-01 00:00:03
    #   #         2001-01-01 00:00:03.500
    #   #         2001-01-01 00:00:04
    #   # ]
    #
    # @example
    #   date.dt.second
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         0
    #   #         1
    #   #         1
    #   #         2
    #   #         2
    #   #         3
    #   #         3
    #   #         4
    #   # ]
    #
    # @example
    #   date.dt.second(fractional: true)
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [f64]
    #   # [
    #   #         0.0
    #   #         0.5
    #   #         1.0
    #   #         1.5
    #   #         2.0
    #   #         2.5
    #   #         3.0
    #   #         3.5
    #   #         4.0
    #   # ]
    def second(fractional: false)
      super
    end

    # Extract the milliseconds from the underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 0, 4)
    #   date = Polars.date_range(start, stop, "500ms")
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00.500
    #   #         2001-01-01 00:00:01
    #   #         2001-01-01 00:00:01.500
    #   #         2001-01-01 00:00:02
    #   #         2001-01-01 00:00:02.500
    #   #         2001-01-01 00:00:03
    #   #         2001-01-01 00:00:03.500
    #   #         2001-01-01 00:00:04
    #   # ]
    #
    # @example
    #   date.dt.millisecond
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         500
    #   #         0
    #   #         500
    #   #         0
    #   #         500
    #   #         0
    #   #         500
    #   #         0
    #   # ]
    def millisecond
      super
    end

    # Extract the microseconds from the underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 0, 4)
    #   date = Polars.date_range(start, stop, "500ms")
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00.500
    #   #         2001-01-01 00:00:01
    #   #         2001-01-01 00:00:01.500
    #   #         2001-01-01 00:00:02
    #   #         2001-01-01 00:00:02.500
    #   #         2001-01-01 00:00:03
    #   #         2001-01-01 00:00:03.500
    #   #         2001-01-01 00:00:04
    #   # ]
    #
    # @example
    #   date.dt.microsecond
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         500000
    #   #         0
    #   #         500000
    #   #         0
    #   #         500000
    #   #         0
    #   #         500000
    #   #         0
    #   # ]
    def microsecond
      super
    end

    # Extract the nanoseconds from the underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 0, 4)
    #   date = Polars.date_range(start, stop, "500ms")
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00.500
    #   #         2001-01-01 00:00:01
    #   #         2001-01-01 00:00:01.500
    #   #         2001-01-01 00:00:02
    #   #         2001-01-01 00:00:02.500
    #   #         2001-01-01 00:00:03
    #   #         2001-01-01 00:00:03.500
    #   #         2001-01-01 00:00:04
    #   # ]
    #
    # @example
    #   date.dt.nanosecond
    #   # =>
    #   # shape: (9,)
    #   # Series: '' [u32]
    #   # [
    #   #         0
    #   #         500000000
    #   #         0
    #   #         500000000
    #   #         0
    #   #         500000000
    #   #         0
    #   #         500000000
    #   #         0
    #   # ]
    def nanosecond
      super
    end

    # Return a timestamp in the given time unit.
    #
    # @param tu ["us", "ns", "ms"]
    #   Time unit.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   date = Polars.date_range(start, stop, "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.timestamp.alias("timestamp_us")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'timestamp_us' [i64]
    #   # [
    #   #         978307200000000
    #   #         978393600000000
    #   #         978480000000000
    #   # ]
    #
    # @example
    #   date.dt.timestamp("ns").alias("timestamp_ns")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'timestamp_ns' [i64]
    #   # [
    #   #         978307200000000000
    #   #         978393600000000000
    #   #         978480000000000000
    #   # ]
    def timestamp(tu = "us")
      super
    end

    # Get the time passed since the Unix EPOCH in the give time unit.
    #
    # @param tu ["us", "ns", "ms", "s", "d"]
    #   Time unit.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   date = Polars.date_range(start, stop, "1d")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [datetime[μs]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.epoch.alias("epoch_ns")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'epoch_ns' [i64]
    #   # [
    #   #         978307200000000
    #   #         978393600000000
    #   #         978480000000000
    #   # ]
    #
    # @example
    #   date.dt.epoch("s").alias("epoch_s")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'epoch_s' [i64]
    #   # [
    #   #         978307200
    #   #         978393600
    #   #         978480000
    #   # ]
    def epoch(tu = "us")
      super
    end

    # def with_time_unit
    # end

    # def cast_time_unit
    # end

    # def with_time_zone
    # end

    # def cast_time_zone
    # end

    # def tz_localize
    # end

    # def days
    # end

    # def hours
    # end

    # def minutes
    # end

    # def seconds
    # end

    # def milliseconds
    # end

    # def microseconds
    # end

    # def nanoseconds
    # end

    # def offset_by
    # end

    # def truncate
    # end

    # def round
    # end
  end
end

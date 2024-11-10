module Polars
  # Series.dt namespace.
  class DateTimeNameSpace
    include ExprDispatch

    self._accessor = "dt"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Get item.
    #
    # @return [Object]
    def [](item)
      s = Utils.wrap_s(_s)
      s[item]
    end

    # Return minimum as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   s = Polars::Series.new([Date.new(2001, 1, 1), Date.new(2001, 1, 2), Date.new(2001, 1, 3)])
    #   s.dt.min
    #   # => Mon, 01 Jan 2001
    def min
      Utils.wrap_s(_s).min
    end

    # Return maximum as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   s = Polars::Series.new([Date.new(2001, 1, 1), Date.new(2001, 1, 2), Date.new(2001, 1, 3)])
    #   s.dt.max
    #   # => Wed, 03 Jan 2001
    def max
      Utils.wrap_s(_s).max
    end

    # Return median as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   date = Polars.datetime_range(
    #     DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d", eager: true
    #   ).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns]]
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
      _s.median
    end

    # Return mean as Ruby object.
    #
    # @return [Object]
    #
    # @example
    #   s = Polars::Series.new([Date.new(2001, 1, 1), Date.new(2001, 1, 2)])
    #   s.dt.mean
    #   # => 2001-01-01 12:00:00 UTC
    #
    # @example
    #   s = Polars::Series.new(
    #     [DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 2), DateTime.new(2001, 1, 3)]
    #   )
    #   s.dt.mean
    #   # => 2001-01-02 00:00:00 UTC
    def mean
      _s.mean
    end

    # Convert a Date/Time/Datetime column into a String column with the given format.
    #
    # Similar to `cast(Polars::String)`, but this method allows you to customize the
    # formatting of the resulting string.
    #
    # @param format [String]
    #   Format to use, refer to the `chrono strftime documentation
    #   <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
    #   for specification. Example: `"%y-%m-%d"`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "datetime",
    #     [DateTime.new(2020, 3, 1), DateTime.new(2020, 4, 1), DateTime.new(2020, 5, 1)],
    #   )
    #   s.dt.to_string("%Y/%m/%d")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [str]
    #   # [
    #   #         "2020/03/01"
    #   #         "2020/04/01"
    #   #         "2020/05/01"
    #   # ]
    def to_string(format)
      super
    end

    # Format Date/datetime with a formatting rule.
    #
    # See [chrono strftime/strptime](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "datetime",
    #     [DateTime.new(2020, 3, 1), DateTime.new(2020, 4, 1), DateTime.new(2020, 5, 1)]
    #   )
    #   s.dt.strftime("%Y/%m/%d")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [str]
    #   # [
    #   #         "2020/03/01"
    #   #         "2020/04/01"
    #   #         "2020/05/01"
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
    #   s = Polars::Series.new("date", [Date.new(2001, 1, 1), Date.new(2002, 1, 1)])
    #   s.dt.year
    #   # =>
    #   # shape: (2,)
    #   # Series: 'date' [i32]
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
    #   date = Polars.date_range(
    #     Date.new(2001, 1, 1), Date.new(2001, 4, 1), "1mo", eager: true
    #   ).alias("date")
    #   date.dt.quarter
    #   # =>
    #   # shape: (4,)
    #   # Series: 'date' [i8]
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
    #   date = Polars.date_range(
    #     Date.new(2001, 1, 1), Date.new(2001, 4, 1), "1mo", eager: true
    #   ).alias("date")
    #   date.dt.month
    #   # =>
    #   # shape: (4,)
    #   # Series: 'date' [i8]
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
    #   date = Polars.date_range(
    #     Date.new(2001, 1, 1), Date.new(2001, 4, 1), "1mo", eager: true
    #   ).alias("date")
    #   date.dt.week
    #   # =>
    #   # shape: (4,)
    #   # Series: 'date' [i8]
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
    # Returns the ISO weekday number where monday = 1 and sunday = 7
    #
    # @return [Series]
    #
    # @example
    #   s = Polars.date_range(Date.new(2001, 1, 1), Date.new(2001, 1, 7), eager: true).alias(
    #     "date"
    #   )
    #   s.dt.weekday
    #   # =>
    #   # shape: (7,)
    #   # Series: 'date' [i8]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   #         5
    #   #         6
    #   #         7
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
    #   s = Polars.date_range(
    #     Date.new(2001, 1, 1), Date.new(2001, 1, 9), "2d", eager: true
    #   ).alias("date")
    #   s.dt.day
    #   # =>
    #   # shape: (5,)
    #   # Series: 'date' [i8]
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
    #   s = Polars.date_range(
    #     Date.new(2001, 1, 1), Date.new(2001, 3, 1), "1mo", eager: true
    #   ).alias("date")
    #   s.dt.ordinal_day
    #   # =>
    #   # shape: (3,)
    #   # Series: 'date' [i16]
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
    #   date = Polars.datetime_range(start, stop, "1h", eager: true).alias("datetime")
    #   date.dt.hour
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [i8]
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
    #   date = Polars.datetime_range(start, stop, "2m", eager: true).alias("datetime")
    #   date.dt.minute
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [i8]
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
    #   date = Polars.datetime_range(start, stop, "500ms", eager: true).alias("datetime")
    #   date.dt.second
    #   # =>
    #   # shape: (9,)
    #   # Series: 'datetime' [i8]
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
    #   # Series: 'datetime' [f64]
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
    #   date = Polars.datetime_range(start, stop, "500ms", eager: true).alias("datetime")
    #   date.dt.millisecond
    #   # =>
    #   # shape: (9,)
    #   # Series: 'datetime' [i32]
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
    #   date = Polars.datetime_range(start, stop, "500ms", eager: true).alias("datetime")
    #   date.dt.microsecond
    #   # =>
    #   # shape: (9,)
    #   # Series: 'datetime' [i32]
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
    #   date = Polars.datetime_range(start, stop, "500ms", eager: true).alias("datetime")
    #   date.dt.nanosecond
    #   # =>
    #   # shape: (9,)
    #   # Series: 'datetime' [i32]
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
    # @param time_unit ["us", "ns", "ms"]
    #   Time unit.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   date = Polars.datetime_range(start, stop, "1d", eager: true).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns]]
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
    def timestamp(time_unit = "us")
      super
    end

    # Get the time passed since the Unix EPOCH in the give time unit.
    #
    # @param time_unit ["us", "ns", "ms", "s", "d"]
    #   Time unit.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   date = Polars.datetime_range(start, stop, "1d", eager: true).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns]]
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
    def epoch(time_unit = "us")
      super
    end

    # Set time unit a Series of dtype Datetime or Duration.
    #
    # This does not modify underlying data, and should be used to fix an incorrect
    # time unit.
    #
    # @param time_unit ["ns", "us", "ms"]
    #   Time unit for the `Datetime` Series.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   date = Polars.datetime_range(start, stop, "1d", time_unit: "ns", eager: true).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.with_time_unit("us").alias("tu_us")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'tu_us' [datetime[μs]]
    #   # [
    #   #         +32971-04-28 00:00:00
    #   #         +32974-01-22 00:00:00
    #   #         +32976-10-18 00:00:00
    #   # ]
    def with_time_unit(time_unit)
      super
    end

    # Cast the underlying data to another time unit. This may lose precision.
    #
    # @param time_unit ["ns", "us", "ms"]
    #   Time unit for the `Datetime` Series.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   date = Polars.datetime_range(start, stop, "1d", eager: true).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.cast_time_unit("ms").alias("tu_ms")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'tu_ms' [datetime[ms]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    #
    # @example
    #   date.dt.cast_time_unit("ns").alias("tu_ns")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'tu_ns' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-02 00:00:00
    #   #         2001-01-03 00:00:00
    #   # ]
    def cast_time_unit(time_unit)
      super
    end

    # Set time zone a Series of type Datetime.
    #
    # @param time_zone [String]
    #   Time zone for the `Datetime` Series.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2020, 3, 1)
    #   stop = DateTime.new(2020, 5, 1)
    #   date = Polars.datetime_range(start, stop, "1mo", time_zone: "UTC", eager: true).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns, UTC]]
    #   # [
    #   #         2020-03-01 00:00:00 UTC
    #   #         2020-04-01 00:00:00 UTC
    #   #         2020-05-01 00:00:00 UTC
    #   # ]
    #
    # @example
    #   date.dt.convert_time_zone("Europe/London").alias("London")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'London' [datetime[ns, Europe/London]]
    #   # [
    #   #         2020-03-01 00:00:00 GMT
    #   #         2020-04-01 01:00:00 BST
    #   #         2020-05-01 01:00:00 BST
    #   # ]
    def convert_time_zone(time_zone)
      super
    end

    # Cast time zone for a Series of type Datetime.
    #
    # Different from `with_time_zone`, this will also modify
    # the underlying timestamp.
    #
    # @param time_zone [String]
    #     Time zone for the `Datetime` Series. Pass `nil` to unset time zone.
    # @param ambiguous [String]
    #     Determine how to deal with ambiguous datetimes.
    # @param non_existent [String]
    #     Determine how to deal with non-existent datetimes.
    #
    # @return [Series]
    #
    # @example
    #   start = DateTime.new(2020, 3, 1)
    #   stop = DateTime.new(2020, 5, 1)
    #   date = Polars.datetime_range(start, stop, "1mo", time_zone: "UTC", eager: true).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns, UTC]]
    #   # [
    #   #         2020-03-01 00:00:00 UTC
    #   #         2020-04-01 00:00:00 UTC
    #   #         2020-05-01 00:00:00 UTC
    #   # ]
    #
    # @example
    #   date.dt.epoch("s")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         1583020800
    #   #         1585699200
    #   #         1588291200
    #   # ]
    #
    # @example
    #   date = date.dt.convert_time_zone("Europe/London").alias("London")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'London' [datetime[ns, Europe/London]]
    #   # [
    #   #         2020-03-01 00:00:00 GMT
    #   #         2020-04-01 01:00:00 BST
    #   #         2020-05-01 01:00:00 BST
    #   # ]
    #
    # @example Timestamps have not changed after convert_time_zone
    #   date.dt.epoch("s")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'London' [i64]
    #   # [
    #   #         1583020800
    #   #         1585699200
    #   #         1588291200
    #   # ]
    #
    # @example
    #   date = date.dt.replace_time_zone("America/New_York").alias("NYC")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'NYC' [datetime[ns, America/New_York]]
    #   # [
    #   #         2020-03-01 00:00:00 EST
    #   #         2020-04-01 01:00:00 EDT
    #   #         2020-05-01 01:00:00 EDT
    #   # ]
    #
    # @example Timestamps have changed after replace_time_zone
    #   date.dt.epoch("s")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'NYC' [i64]
    #   # [
    #   #         1583038800
    #   #         1585717200
    #   #         1588309200
    #   # ]
    def replace_time_zone(time_zone, ambiguous: "raise", non_existent: "raise")
      super
    end

    # Localize tz-naive Datetime Series to tz-aware Datetime Series.
    #
    # This method takes a naive Datetime Series and makes this time zone aware.
    # It does not move the time to another time zone.
    #
    # @param tz [String]
    #   Time zone for the `Datetime` Series.
    #
    # @return [Series]
    def tz_localize(tz)
      super
    end

    # Extract the days from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(
    #     Time.utc(2020, 3, 1), Time.utc(2020, 5, 1), "1mo", eager: true
    #   ).alias("datetime")
    #   date.diff.dt.total_days
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         31
    #   #         30
    #   # ]
    def total_days
      super
    end
    alias_method :days, :total_days

    # Extract the hours from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 4), "1d", time_unit: "us", eager: true).alias("datetime")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2020-01-01 00:00:00
    #   #         2020-01-02 00:00:00
    #   #         2020-01-03 00:00:00
    #   #         2020-01-04 00:00:00
    #   # ]
    #
    # @example
    #   date.diff.dt.total_hours
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         24
    #   #         24
    #   #         24
    #   # ]
    def total_hours
      super
    end
    alias_method :hours, :total_hours

    # Extract the minutes from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 4), "1d", time_unit: "us", eager: true).alias("datetime")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2020-01-01 00:00:00
    #   #         2020-01-02 00:00:00
    #   #         2020-01-03 00:00:00
    #   #         2020-01-04 00:00:00
    #   # ]
    #
    # @example
    #   date.diff.dt.total_minutes
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         1440
    #   #         1440
    #   #         1440
    #   # ]
    def total_minutes
      super
    end
    alias_method :minutes, :total_minutes

    # Extract the seconds from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(
    #     DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 4, 0), "1m", time_unit: "us", eager: true
    #   ).alias("datetime")
    #   # =>
    #   # shape: (5,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2020-01-01 00:00:00
    #   #         2020-01-01 00:01:00
    #   #         2020-01-01 00:02:00
    #   #         2020-01-01 00:03:00
    #   #         2020-01-01 00:04:00
    #   # ]
    #
    # @example
    #   date.diff.dt.total_seconds
    #   # =>
    #   # shape: (5,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         60
    #   #         60
    #   #         60
    #   #         60
    #   # ]
    def total_seconds
      super
    end
    alias_method :seconds, :total_seconds

    # Extract the milliseconds from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(
    #     DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1, 0), "1ms", time_unit: "us", eager: true
    #   ).alias("datetime")[0..2]
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2020-01-01 00:00:00
    #   #         2020-01-01 00:00:00.001
    #   #         2020-01-01 00:00:00.002
    #   # ]
    #
    # @example
    #   date.diff.dt.total_milliseconds
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         1
    #   #         1
    #   # ]
    def total_milliseconds
      super
    end
    alias_method :milliseconds, :total_milliseconds

    # Extract the microseconds from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(
    #     DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1, 0), "1ms", time_unit: "us", eager: true
    #   ).alias("datetime")[0..2]
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2020-01-01 00:00:00
    #   #         2020-01-01 00:00:00.001
    #   #         2020-01-01 00:00:00.002
    #   # ]
    #
    # @example
    #   date.diff.dt.total_microseconds
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         1000
    #   #         1000
    #   # ]
    def total_microseconds
      super
    end
    alias_method :microseconds, :total_microseconds

    # Extract the nanoseconds from a Duration type.
    #
    # @return [Series]
    #
    # @example
    #   date = Polars.datetime_range(
    #     DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1, 0), "1ms", time_unit: "us", eager: true
    #   ).alias("datetime")[0..2]
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2020-01-01 00:00:00
    #   #         2020-01-01 00:00:00.001
    #   #         2020-01-01 00:00:00.002
    #   # ]
    #
    # @example
    #   date.diff.dt.total_nanoseconds
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [i64]
    #   # [
    #   #         null
    #   #         1000000
    #   #         1000000
    #   # ]
    def total_nanoseconds
      super
    end
    alias_method :nanoseconds, :total_nanoseconds

    # Offset this date by a relative time offset.
    #
    # This differs from `Polars.col("foo") + timedelta` in that it can
    # take months and leap years into account. Note that only a single minus
    # sign is allowed in the `by` string, as the first character.
    #
    # @param by [String]
    #   The offset is dictated by the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    # @return [Series]
    #
    # @example
    #   dates = Polars.datetime_range(
    #     DateTime.new(2000, 1, 1), DateTime.new(2005, 1, 1), "1y", eager: true
    #   ).alias("datetime")
    #   # =>
    #   # shape: (6,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2000-01-01 00:00:00
    #   #         2001-01-01 00:00:00
    #   #         2002-01-01 00:00:00
    #   #         2003-01-01 00:00:00
    #   #         2004-01-01 00:00:00
    #   #         2005-01-01 00:00:00
    #   # ]
    #
    # @example
    #   dates.dt.offset_by("1y").alias("date_plus_1y")
    #   # =>
    #   # shape: (6,)
    #   # Series: 'date_plus_1y' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2002-01-01 00:00:00
    #   #         2003-01-01 00:00:00
    #   #         2004-01-01 00:00:00
    #   #         2005-01-01 00:00:00
    #   #         2006-01-01 00:00:00
    #   # ]
    #
    # @example
    #   dates.dt.offset_by("-1y2mo").alias("date_minus_1y_2mon")
    #   # =>
    #   # shape: (6,)
    #   # Series: 'date_minus_1y_2mon' [datetime[ns]]
    #   # [
    #   #         1998-11-01 00:00:00
    #   #         1999-11-01 00:00:00
    #   #         2000-11-01 00:00:00
    #   #         2001-11-01 00:00:00
    #   #         2002-11-01 00:00:00
    #   #         2003-11-01 00:00:00
    #   # ]
    def offset_by(by)
      super
    end

    # Divide the date/ datetime range into buckets.
    #
    # Each date/datetime is mapped to the start of its bucket.
    #
    # The `every` and `offset` argument are created with the
    # the following string language:
    #
    # 1ns # 1 nanosecond
    # 1us # 1 microsecond
    # 1ms # 1 millisecond
    # 1s  # 1 second
    # 1m  # 1 minute
    # 1h  # 1 hour
    # 1d  # 1 day
    # 1w  # 1 week
    # 1mo # 1 calendar month
    # 1y  # 1 calendar year
    #
    # 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @param every [String]
    #   Every interval start and period length.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars.datetime_range(
    #     Time.utc(2001, 1, 1),
    #     Time.utc(2001, 1, 2),
    #     "165m",
    #     eager: true
    #   ).alias("datetime")
    #   s.dt.truncate("1h")
    #   # =>
    #   # shape: (9,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 02:00:00
    #   #         2001-01-01 05:00:00
    #   #         2001-01-01 08:00:00
    #   #         2001-01-01 11:00:00
    #   #         2001-01-01 13:00:00
    #   #         2001-01-01 16:00:00
    #   #         2001-01-01 19:00:00
    #   #         2001-01-01 22:00:00
    #   # ]
    #
    # @example
    #   s = Polars.datetime_range(
    #     Time.utc(2001, 1, 1), Time.utc(2001, 1, 1, 1), "10m", eager: true
    #   ).alias("datetime")
    #   s.dt.truncate("30m")
    #   # =>
    #   # shape: (7,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:30:00
    #   #         2001-01-01 00:30:00
    #   #         2001-01-01 00:30:00
    #   #         2001-01-01 01:00:00
    #   # ]
    def truncate(every)
      super
    end

    # Divide the date/ datetime range into buckets.
    #
    # Each date/datetime in the first half of the interval
    # is mapped to the start of its bucket.
    # Each date/datetime in the seconod half of the interval
    # is mapped to the end of its bucket.
    #
    # The `every` and `offset` argument are created with the
    # the following string language:
    #
    # 1ns # 1 nanosecond
    # 1us # 1 microsecond
    # 1ms # 1 millisecond
    # 1s  # 1 second
    # 1m  # 1 minute
    # 1h  # 1 hour
    # 1d  # 1 day
    # 1w  # 1 week
    # 1mo # 1 calendar month
    # 1y  # 1 calendar year
    #
    # 3d12h4m25s # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @param every [String]
    #   Every interval start and period length.
    #
    # @return [Series]
    #
    # @note
    #   This functionality is currently experimental and may
    #   change without it being considered a breaking change.
    #
    # @example
    #   start = Time.utc(2001, 1, 1)
    #   stop = Time.utc(2001, 1, 2)
    #   s = Polars.datetime_range(
    #     start, stop, "165m", eager: true
    #   ).alias("datetime")
    #   s.dt.round("1h")
    #   # =>
    #   # shape: (9,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 03:00:00
    #   #         2001-01-01 06:00:00
    #   #         2001-01-01 08:00:00
    #   #         2001-01-01 11:00:00
    #   #         2001-01-01 14:00:00
    #   #         2001-01-01 17:00:00
    #   #         2001-01-01 19:00:00
    #   #         2001-01-01 22:00:00
    #   # ]
    #
    # @example
    #   round_str = s.dt.round("1h")
    #   round_td = s.dt.round("1h")
    #   round_str.equals(round_td)
    #   # => true
    #
    # @example
    #   start = Time.utc(2001, 1, 1)
    #   stop = Time.utc(2001, 1, 1, 1)
    #   s = Polars.datetime_range(start, stop, "10m", eager: true).alias("datetime")
    #   s.dt.round("30m")
    #   # =>
    #   # shape: (7,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:00:00
    #   #         2001-01-01 00:30:00
    #   #         2001-01-01 00:30:00
    #   #         2001-01-01 00:30:00
    #   #         2001-01-01 01:00:00
    #   #         2001-01-01 01:00:00
    #   # ]
    def round(every)
      super
    end

    # Roll backward to the first day of the month.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars.datetime_range(
    #     DateTime.new(2000, 1, 2, 2), DateTime.new(2000, 4, 2, 2), "1mo", time_unit: "us", eager: true
    #   ).alias("datetime")
    #   s.dt.month_start
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2000-01-01 02:00:00
    #   #         2000-02-01 02:00:00
    #   #         2000-03-01 02:00:00
    #   #         2000-04-01 02:00:00
    #   # ]
    def month_start
      super
    end

    # Roll forward to the last day of the month.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars.datetime_range(
    #     DateTime.new(2000, 1, 2, 2), DateTime.new(2000, 4, 2, 2), "1mo", time_unit: "us", eager: true
    #   ).alias("datetime")
    #   s.dt.month_end
    #   # =>
    #   # shape: (4,)
    #   # Series: 'datetime' [datetime[μs]]
    #   # [
    #   #         2000-01-31 02:00:00
    #   #         2000-02-29 02:00:00
    #   #         2000-03-31 02:00:00
    #   #         2000-04-30 02:00:00
    #   # ]
    def month_end
      super
    end

    # Base offset from UTC.
    #
    # This is usually constant for all datetimes in a given time zone, but
    # may vary in the rare case that a country switches time zone, like
    # Samoa (Apia) did at the end of 2011.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars.datetime_range(
    #     DateTime.new(2011, 12, 29),
    #     DateTime.new(2012, 1, 1),
    #     "2d",
    #     time_zone: "Pacific/Apia",
    #     eager: true,
    #   ).alias("datetime")
    #   s.dt.base_utc_offset
    #   # =>
    #   # shape: (2,)
    #   # Series: 'datetime' [duration[ms]]
    #   # [
    #   #         -11h
    #   #         13h
    #   # ]
    def base_utc_offset
      super
    end

    # Additional offset currently in effect (typically due to daylight saving time).
    #
    # @return [Series]
    #
    # @example
    #   s = Polars.datetime_range(
    #     DateTime.new(2020, 10, 25),
    #     DateTime.new(2020, 10, 26),
    #     time_zone: "Europe/London",
    #     eager: true,
    #   ).alias("datetime")
    #   s.dt.dst_offset
    #   # =>
    #   # shape: (2,)
    #   # Series: 'datetime' [duration[ms]]
    #   # [
    #   #         1h
    #   #         0ms
    #   # ]
    def dst_offset
      super
    end
  end
end

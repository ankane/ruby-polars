module Polars
  # Namespace for datetime related expressions.
  class DateTimeExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Divide the date/datetime range into buckets.
    #
    # Each date/datetime is mapped to the start of its bucket.
    #
    # @param every [String]
    #   Every interval start and period length
    # @param offset [String]
    #   Offset the window
    #
    # @return [Expr]
    #
    # @note
    #   The `every` and `offset` argument are created with the
    #   the following small string formatting language:
    #
    #   1ns  # 1 nanosecond
    #   1us  # 1 microsecond
    #   1ms  # 1 millisecond
    #   1s   # 1 second
    #   1m   # 1 minute
    #   1h   # 1 hour
    #   1d   # 1 day
    #   1w   # 1 week
    #   1mo  # 1 calendar month
    #   1y   # 1 calendar year
    #
    #   eg: 3d12h4m25s  # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df = Polars.date_range(
    #     start, stop, "225m", name: "dates"
    #   ).to_frame
    #   # =>
    #   # shape: (7, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 03:45:00 │
    #   # │ 2001-01-01 07:30:00 │
    #   # │ 2001-01-01 11:15:00 │
    #   # │ 2001-01-01 15:00:00 │
    #   # │ 2001-01-01 18:45:00 │
    #   # │ 2001-01-01 22:30:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("dates").dt.truncate("1h"))
    #   # =>
    #   # shape: (7, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 03:00:00 │
    #   # │ 2001-01-01 07:00:00 │
    #   # │ 2001-01-01 11:00:00 │
    #   # │ 2001-01-01 15:00:00 │
    #   # │ 2001-01-01 18:00:00 │
    #   # │ 2001-01-01 22:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 1)
    #   df = Polars.date_range(start, stop, "10m", name: "dates").to_frame
    #   df.select(["dates", Polars.col("dates").dt.truncate("30m").alias("truncate")])
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ dates               ┆ truncate            │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[μs]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:10:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:20:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:30:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:40:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:50:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 01:00:00 ┆ 2001-01-01 01:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def truncate(every, offset: nil)
      if offset.nil?
        offset = "0ns"
      end

      Utils.wrap_expr(
        _rbexpr.dt_truncate(
          Utils._timedelta_to_pl_duration(every),
          Utils._timedelta_to_pl_duration(offset)
        )
      )
    end

    # Divide the date/datetime range into buckets.
    #
    # Each date/datetime in the first half of the interval
    # is mapped to the start of its bucket.
    # Each date/datetime in the seconod half of the interval
    # is mapped to the end of its bucket.
    #
    # @param every [String]
    #   Every interval start and period length
    # @param offset [String]
    #   Offset the window
    #
    # @return [Expr]
    #
    # @note
    #   The `every` and `offset` argument are created with the
    #   the following small string formatting language:
    #
    #   1ns  # 1 nanosecond
    #   1us  # 1 microsecond
    #   1ms  # 1 millisecond
    #   1s   # 1 second
    #   1m   # 1 minute
    #   1h   # 1 hour
    #   1d   # 1 day
    #   1w   # 1 week
    #   1mo  # 1 calendar month
    #   1y   # 1 calendar year
    #
    #   eg: 3d12h4m25s  # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @note
    #   This functionality is currently experimental and may
    #   change without it being considered a breaking change.
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df = Polars.date_range(
    #     start, stop, "225m", name: "dates"
    #   ).to_frame
    #   # =>
    #   # shape: (7, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 03:45:00 │
    #   # │ 2001-01-01 07:30:00 │
    #   # │ 2001-01-01 11:15:00 │
    #   # │ 2001-01-01 15:00:00 │
    #   # │ 2001-01-01 18:45:00 │
    #   # │ 2001-01-01 22:30:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("dates").dt.round("1h"))
    #   # =>
    #   # shape: (7, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 04:00:00 │
    #   # │ 2001-01-01 08:00:00 │
    #   # │ 2001-01-01 11:00:00 │
    #   # │ 2001-01-01 15:00:00 │
    #   # │ 2001-01-01 19:00:00 │
    #   # │ 2001-01-01 23:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 1)
    #   df = Polars.date_range(start, stop, "10m", name: "dates").to_frame
    #   df.select(["dates", Polars.col("dates").dt.round("30m").alias("round")])
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ dates               ┆ round               │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[μs]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:10:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:20:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:30:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:40:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:50:00 ┆ 2001-01-01 01:00:00 │
    #   # │ 2001-01-01 01:00:00 ┆ 2001-01-01 01:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def round(every, offset: nil)
      if offset.nil?
        offset = "0ns"
      end

      Utils.wrap_expr(
        _rbexpr.dt_round(
          Utils._timedelta_to_pl_duration(every),
          Utils._timedelta_to_pl_duration(offset)
        )
      )
    end

    # Create a naive Datetime from an existing Date/Datetime expression and a Time.
    #
    # If the underlying expression is a Datetime then its time component is replaced,
    # and if it is a Date then a new Datetime is created by combining the two values.
    #
    # @param time [Object]
    #   A Ruby time literal or Polars expression/column that resolves to a time.
    # @param time_unit ["ns", "us", "ms"]
    #   Unit of time.
    #
    # @return [Expr]
    def combine(time, time_unit: "us")
      unless time.is_a?(Time) || time.is_a?(Expr)
        raise TypeError, "expected 'time' to be a Ruby time or Polars expression, found #{time}"
      end
      time = Utils.expr_to_lit_or_expr(time)
      Utils.wrap_expr(_rbexpr.dt_combine(time._rbexpr, time_unit))
    end

    # Format Date/datetime with a formatting rule.
    #
    # See [chrono strftime/strptime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    #
    # @return [Expr]
    def strftime(fmt)
      Utils.wrap_expr(_rbexpr.strftime(fmt))
    end

    # Extract year from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number in the calendar date.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2002, 7, 1)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "180d")})
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-06-30 00:00:00 │
    #   # │ 2001-12-27 00:00:00 │
    #   # │ 2002-06-25 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.year)
    #   # =>
    #   # shape: (4, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ i32  │
    #   # ╞══════╡
    #   # │ 2001 │
    #   # │ 2001 │
    #   # │ 2001 │
    #   # │ 2002 │
    #   # └──────┘
    def year
      Utils.wrap_expr(_rbexpr.year)
    end

    # Extract ISO year from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number in the ISO standard.
    # This may not correspond with the calendar year.
    #
    # @return [Expr]
    def iso_year
      Utils.wrap_expr(_rbexpr.iso_year)
    end

    # Extract quarter from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the quarter ranging from 1 to 4.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2002, 6, 1)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "180d")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-06-30 00:00:00 │
    #   # │ 2001-12-27 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.quarter)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 1    │
    #   # │ 2    │
    #   # │ 4    │
    #   # └──────┘
    def quarter
      Utils.wrap_expr(_rbexpr.quarter)
    end

    # Extract month from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the month number starting from 1.
    # The return value ranges from 1 to 12.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 4, 1)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "31d")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-02-01 00:00:00 │
    #   # │ 2001-03-04 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.month)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 1    │
    #   # │ 2    │
    #   # │ 3    │
    #   # └──────┘
    def month
      Utils.wrap_expr(_rbexpr.month)
    end

    # Extract the week from the underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the ISO week number starting from 1.
    # The return value ranges from 1 to 53. (The last week of year differs by years.)
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 4, 1)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "31d")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-02-01 00:00:00 │
    #   # │ 2001-03-04 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.week)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 1    │
    #   # │ 5    │
    #   # │ 9    │
    #   # └──────┘
    def week
      Utils.wrap_expr(_rbexpr.week)
    end

    # Extract the week day from the underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the ISO weekday number where monday = 1 and sunday = 7
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 9)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "3d")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-04 00:00:00 │
    #   # │ 2001-01-07 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(
    #     [
    #       Polars.col("date").dt.weekday.alias("weekday"),
    #       Polars.col("date").dt.day.alias("day_of_month"),
    #       Polars.col("date").dt.ordinal_day.alias("day_of_year")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────┬──────────────┬─────────────┐
    #   # │ weekday ┆ day_of_month ┆ day_of_year │
    #   # │ ---     ┆ ---          ┆ ---         │
    #   # │ u32     ┆ u32          ┆ u32         │
    #   # ╞═════════╪══════════════╪═════════════╡
    #   # │ 1       ┆ 1            ┆ 1           │
    #   # │ 4       ┆ 4            ┆ 4           │
    #   # │ 7       ┆ 7            ┆ 7           │
    #   # └─────────┴──────────────┴─────────────┘
    def weekday
      Utils.wrap_expr(_rbexpr.weekday)
    end

    # Extract day from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the day of month starting from 1.
    # The return value ranges from 1 to 31. (The last day of month differs by months.)
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 9)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "3d")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-04 00:00:00 │
    #   # │ 2001-01-07 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(
    #     [
    #       Polars.col("date").dt.weekday.alias("weekday"),
    #       Polars.col("date").dt.day.alias("day_of_month"),
    #       Polars.col("date").dt.ordinal_day.alias("day_of_year")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────┬──────────────┬─────────────┐
    #   # │ weekday ┆ day_of_month ┆ day_of_year │
    #   # │ ---     ┆ ---          ┆ ---         │
    #   # │ u32     ┆ u32          ┆ u32         │
    #   # ╞═════════╪══════════════╪═════════════╡
    #   # │ 1       ┆ 1            ┆ 1           │
    #   # │ 4       ┆ 4            ┆ 4           │
    #   # │ 7       ┆ 7            ┆ 7           │
    #   # └─────────┴──────────────┴─────────────┘
    def day
      Utils.wrap_expr(_rbexpr.day)
    end

    # Extract ordinal day from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the day of month starting from 1.
    # The return value ranges from 1 to 31. (The last day of month differs by months.)
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 9)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "3d")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-04 00:00:00 │
    #   # │ 2001-01-07 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(
    #     [
    #       Polars.col("date").dt.weekday.alias("weekday"),
    #       Polars.col("date").dt.day.alias("day_of_month"),
    #       Polars.col("date").dt.ordinal_day.alias("day_of_year")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────┬──────────────┬─────────────┐
    #   # │ weekday ┆ day_of_month ┆ day_of_year │
    #   # │ ---     ┆ ---          ┆ ---         │
    #   # │ u32     ┆ u32          ┆ u32         │
    #   # ╞═════════╪══════════════╪═════════════╡
    #   # │ 1       ┆ 1            ┆ 1           │
    #   # │ 4       ┆ 4            ┆ 4           │
    #   # │ 7       ┆ 7            ┆ 7           │
    #   # └─────────┴──────────────┴─────────────┘
    def ordinal_day
      Utils.wrap_expr(_rbexpr.ordinal_day)
    end

    # Extract hour from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # Returns the hour number from 0 to 23.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "12h")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 12:00:00 │
    #   # │ 2001-01-02 00:00:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.hour)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 0    │
    #   # │ 12   │
    #   # │ 0    │
    #   # └──────┘
    def hour
      Utils.wrap_expr(_rbexpr.hour)
    end

    # Extract minutes from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # Returns the minute number from 0 to 59.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 4, 0)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "2m")})
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:02:00 │
    #   # │ 2001-01-01 00:04:00 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.minute)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 0    │
    #   # │ 2    │
    #   # │ 4    │
    #   # └──────┘
    def minute
      Utils.wrap_expr(_rbexpr.minute)
    end

    # Extract seconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # Returns the integer second number from 0 to 59, or a floating
    # point number from 0 < 60 if `fractional: true` that includes
    # any milli/micro/nanosecond component.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2001, 1, 1, 0, 0, 0.456789),
    #         DateTime.new(2001, 1, 1, 0, 0, 6),
    #         "2s654321us"
    #       )
    #     }
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌────────────────────────────┐
    #   # │ date                       │
    #   # │ ---                        │
    #   # │ datetime[μs]               │
    #   # ╞════════════════════════════╡
    #   # │ 2001-01-01 00:00:00.456789 │
    #   # │ 2001-01-01 00:00:03.111110 │
    #   # │ 2001-01-01 00:00:05.765431 │
    #   # └────────────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.second.alias("secs"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ secs │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 0    │
    #   # │ 3    │
    #   # │ 5    │
    #   # └──────┘
    #
    #   df.select(Polars.col("date").dt.second(fractional: true).alias("secs"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ secs     │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.456789 │
    #   # │ 3.11111  │
    #   # │ 5.765431 │
    #   # └──────────┘
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 1, 0, 0, 4)
    #   df = Polars::DataFrame.new(
    #     {"date" => Polars.date_range(start, stop, "2s")}
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────────┐
    #   # │ date                │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:00:02 │
    #   # │ 2001-01-01 00:00:04 │
    #   # └─────────────────────┘
    #
    # @example
    #   df.select(Polars.col("date").dt.second)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ date │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 0    │
    #   # │ 2    │
    #   # │ 4    │
    #   # └──────┘
    def second(fractional: false)
      sec = Utils.wrap_expr(_rbexpr.second)
      if fractional
        sec + (Utils.wrap_expr(_rbexpr.nanosecond) / Utils.lit(1_000_000_000.0))
      else
        sec
      end
    end

    # Extract milliseconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Expr]
    def millisecond
      Utils.wrap_expr(_rbexpr.millisecond)
    end

    # Extract microseconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Expr]
    def microsecond
      Utils.wrap_expr(_rbexpr.microsecond)
    end

    # Extract nanoseconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Expr]
    def nanosecond
      Utils.wrap_expr(_rbexpr.nanosecond)
    end

    # Get the time passed since the Unix EPOCH in the give time unit.
    #
    # @param tu ["us", "ns", "ms", "s", "d"]
    #   Time unit.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "1d")})
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").dt.epoch.alias("epoch_ns"),
    #       Polars.col("date").dt.epoch("s").alias("epoch_s")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────────────────┬─────────────────┬───────────┐
    #   # │ date                ┆ epoch_ns        ┆ epoch_s   │
    #   # │ ---                 ┆ ---             ┆ ---       │
    #   # │ datetime[μs]        ┆ i64             ┆ i64       │
    #   # ╞═════════════════════╪═════════════════╪═══════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 978307200000000 ┆ 978307200 │
    #   # │ 2001-01-02 00:00:00 ┆ 978393600000000 ┆ 978393600 │
    #   # │ 2001-01-03 00:00:00 ┆ 978480000000000 ┆ 978480000 │
    #   # └─────────────────────┴─────────────────┴───────────┘
    def epoch(tu = "us")
      if Utils::DTYPE_TEMPORAL_UNITS.include?(tu)
        timestamp(tu)
      elsif tu == "s"
        Utils.wrap_expr(_rbexpr.dt_epoch_seconds)
      elsif tu == "d"
        Utils.wrap_expr(_rbexpr).cast(:date).cast(:i32)
      else
        raise ArgumentError, "tu must be one of {{'ns', 'us', 'ms', 's', 'd'}}, got #{tu}"
      end
    end

    # Return a timestamp in the given time unit.
    #
    # @param tu ["us", "ns", "ms"]
    #   Time unit.
    #
    # @return [Expr]
    #
    # @example
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 3)
    #   df = Polars::DataFrame.new({"date" => Polars.date_range(start, stop, "1d")})
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").dt.timestamp.alias("timestamp_ns"),
    #       Polars.col("date").dt.timestamp("ms").alias("timestamp_ms")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────────────────┬─────────────────┬──────────────┐
    #   # │ date                ┆ timestamp_ns    ┆ timestamp_ms │
    #   # │ ---                 ┆ ---             ┆ ---          │
    #   # │ datetime[μs]        ┆ i64             ┆ i64          │
    #   # ╞═════════════════════╪═════════════════╪══════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 978307200000000 ┆ 978307200000 │
    #   # │ 2001-01-02 00:00:00 ┆ 978393600000000 ┆ 978393600000 │
    #   # │ 2001-01-03 00:00:00 ┆ 978480000000000 ┆ 978480000000 │
    #   # └─────────────────────┴─────────────────┴──────────────┘
    def timestamp(tu = "us")
      Utils.wrap_expr(_rbexpr.timestamp(tu))
    end

    # Set time unit of a Series of dtype Datetime or Duration.
    #
    # This does not modify underlying data, and should be used to fix an incorrect
    # time unit.
    #
    # @param tu ["ns", "us", "ms"]
    #   Time unit for the `Datetime` Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d", time_unit: "ns"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").dt.with_time_unit("us").alias("tu_us")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────┬───────────────────────┐
    #   # │ date                ┆ tu_us                 │
    #   # │ ---                 ┆ ---                   │
    #   # │ datetime[ns]        ┆ datetime[μs]          │
    #   # ╞═════════════════════╪═══════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ +32971-04-28 00:00:00 │
    #   # │ 2001-01-02 00:00:00 ┆ +32974-01-22 00:00:00 │
    #   # │ 2001-01-03 00:00:00 ┆ +32976-10-18 00:00:00 │
    #   # └─────────────────────┴───────────────────────┘
    def with_time_unit(tu)
      Utils.wrap_expr(_rbexpr.dt_with_time_unit(tu))
    end

    # Cast the underlying data to another time unit. This may lose precision.
    #
    # @param tu ["ns", "us", "ms"]
    #   Time unit for the `Datetime` Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").dt.cast_time_unit("ms").alias("tu_ms"),
    #       Polars.col("date").dt.cast_time_unit("ns").alias("tu_ns")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┐
    #   # │ date                ┆ tu_ms               ┆ tu_ns               │
    #   # │ ---                 ┆ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[ms]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-02 00:00:00 ┆ 2001-01-02 00:00:00 ┆ 2001-01-02 00:00:00 │
    #   # │ 2001-01-03 00:00:00 ┆ 2001-01-03 00:00:00 ┆ 2001-01-03 00:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┘
    def cast_time_unit(tu)
      Utils.wrap_expr(_rbexpr.dt_cast_time_unit(tu))
    end

    # Set time zone for a Series of type Datetime.
    #
    # @param tz [String]
    #   Time zone for the `Datetime` Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 3, 1),
    #         DateTime.new(2020, 5, 1),
    #         "1mo",
    #         time_zone: "UTC"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date")
    #         .dt.convert_time_zone("Europe/London")
    #         .alias("London")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────────┬─────────────────────────────┐
    #   # │ date                    ┆ London                      │
    #   # │ ---                     ┆ ---                         │
    #   # │ datetime[μs, UTC]       ┆ datetime[μs, Europe/London] │
    #   # ╞═════════════════════════╪═════════════════════════════╡
    #   # │ 2020-03-01 00:00:00 UTC ┆ 2020-03-01 00:00:00 GMT     │
    #   # │ 2020-04-01 00:00:00 UTC ┆ 2020-04-01 01:00:00 BST     │
    #   # │ 2020-05-01 00:00:00 UTC ┆ 2020-05-01 01:00:00 BST     │
    #   # └─────────────────────────┴─────────────────────────────┘
    def convert_time_zone(tz)
      Utils.wrap_expr(_rbexpr.dt_convert_time_zone(tz))
    end

    # Cast time zone for a Series of type Datetime.
    #
    # Different from `convert_time_zone`, this will also modify
    # the underlying timestamp,
    #
    # @param tz [String]
    #   Time zone for the `Datetime` Series.
    #
    # @return [Expr]
    def replace_time_zone(tz, use_earliest: nil)
      Utils.wrap_expr(_rbexpr.dt_replace_time_zone(tz, use_earliest))
    end

    # Localize tz-naive Datetime Series to tz-aware Datetime Series.
    #
    # This method takes a naive Datetime Series and makes this time zone aware.
    # It does not move the time to another time zone.
    #
    # @param tz [String]
    #   Time zone for the `Datetime` Series.
    #
    # @return [Expr]
    def tz_localize(tz)
      Utils.wrap_expr(_rbexpr.dt_tz_localize(tz))
    end

    # Extract the days from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 3, 1), DateTime.new(2020, 5, 1), "1mo"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.days.alias("days_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────┬───────────┐
    #   # │ date                ┆ days_diff │
    #   # │ ---                 ┆ ---       │
    #   # │ datetime[μs]        ┆ i64       │
    #   # ╞═════════════════════╪═══════════╡
    #   # │ 2020-03-01 00:00:00 ┆ null      │
    #   # │ 2020-04-01 00:00:00 ┆ 31        │
    #   # │ 2020-05-01 00:00:00 ┆ 30        │
    #   # └─────────────────────┴───────────┘
    def days
      Utils.wrap_expr(_rbexpr.duration_days)
    end

    # Extract the hours from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 4), "1d"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.hours.alias("hours_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────────────┬────────────┐
    #   # │ date                ┆ hours_diff │
    #   # │ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ i64        │
    #   # ╞═════════════════════╪════════════╡
    #   # │ 2020-01-01 00:00:00 ┆ null       │
    #   # │ 2020-01-02 00:00:00 ┆ 24         │
    #   # │ 2020-01-03 00:00:00 ┆ 24         │
    #   # │ 2020-01-04 00:00:00 ┆ 24         │
    #   # └─────────────────────┴────────────┘
    def hours
      Utils.wrap_expr(_rbexpr.duration_hours)
    end

    # Extract the minutes from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 4), "1d"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.minutes.alias("minutes_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────────────┬──────────────┐
    #   # │ date                ┆ minutes_diff │
    #   # │ ---                 ┆ ---          │
    #   # │ datetime[μs]        ┆ i64          │
    #   # ╞═════════════════════╪══════════════╡
    #   # │ 2020-01-01 00:00:00 ┆ null         │
    #   # │ 2020-01-02 00:00:00 ┆ 1440         │
    #   # │ 2020-01-03 00:00:00 ┆ 1440         │
    #   # │ 2020-01-04 00:00:00 ┆ 1440         │
    #   # └─────────────────────┴──────────────┘
    def minutes
      Utils.wrap_expr(_rbexpr.duration_minutes)
    end

    # Extract the seconds from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 4, 0), "1m"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.seconds.alias("seconds_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────────────────┬──────────────┐
    #   # │ date                ┆ seconds_diff │
    #   # │ ---                 ┆ ---          │
    #   # │ datetime[μs]        ┆ i64          │
    #   # ╞═════════════════════╪══════════════╡
    #   # │ 2020-01-01 00:00:00 ┆ null         │
    #   # │ 2020-01-01 00:01:00 ┆ 60           │
    #   # │ 2020-01-01 00:02:00 ┆ 60           │
    #   # │ 2020-01-01 00:03:00 ┆ 60           │
    #   # │ 2020-01-01 00:04:00 ┆ 60           │
    #   # └─────────────────────┴──────────────┘
    def seconds
      Utils.wrap_expr(_rbexpr.duration_seconds)
    end

    # Extract the milliseconds from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1), "1ms"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.milliseconds.alias("milliseconds_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (1_001, 2)
    #   # ┌─────────────────────────┬───────────────────┐
    #   # │ date                    ┆ milliseconds_diff │
    #   # │ ---                     ┆ ---               │
    #   # │ datetime[μs]            ┆ i64               │
    #   # ╞═════════════════════════╪═══════════════════╡
    #   # │ 2020-01-01 00:00:00     ┆ null              │
    #   # │ 2020-01-01 00:00:00.001 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.002 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.003 ┆ 1                 │
    #   # │ …                       ┆ …                 │
    #   # │ 2020-01-01 00:00:00.997 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.998 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.999 ┆ 1                 │
    #   # │ 2020-01-01 00:00:01     ┆ 1                 │
    #   # └─────────────────────────┴───────────────────┘
    def milliseconds
      Utils.wrap_expr(_rbexpr.duration_milliseconds)
    end

    # Extract the microseconds from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1), "1ms"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.microseconds.alias("microseconds_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (1_001, 2)
    #   # ┌─────────────────────────┬───────────────────┐
    #   # │ date                    ┆ microseconds_diff │
    #   # │ ---                     ┆ ---               │
    #   # │ datetime[μs]            ┆ i64               │
    #   # ╞═════════════════════════╪═══════════════════╡
    #   # │ 2020-01-01 00:00:00     ┆ null              │
    #   # │ 2020-01-01 00:00:00.001 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.002 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.003 ┆ 1000              │
    #   # │ …                       ┆ …                 │
    #   # │ 2020-01-01 00:00:00.997 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.998 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.999 ┆ 1000              │
    #   # │ 2020-01-01 00:00:01     ┆ 1000              │
    #   # └─────────────────────────┴───────────────────┘
    def microseconds
      Utils.wrap_expr(_rbexpr.duration_microseconds)
    end

    # Extract the nanoseconds from a Duration type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1), "1ms"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("date"),
    #       Polars.col("date").diff.dt.nanoseconds.alias("nanoseconds_diff")
    #     ]
    #   )
    #   # =>
    #   # shape: (1_001, 2)
    #   # ┌─────────────────────────┬──────────────────┐
    #   # │ date                    ┆ nanoseconds_diff │
    #   # │ ---                     ┆ ---              │
    #   # │ datetime[μs]            ┆ i64              │
    #   # ╞═════════════════════════╪══════════════════╡
    #   # │ 2020-01-01 00:00:00     ┆ null             │
    #   # │ 2020-01-01 00:00:00.001 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.002 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.003 ┆ 1000000          │
    #   # │ …                       ┆ …                │
    #   # │ 2020-01-01 00:00:00.997 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.998 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.999 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:01     ┆ 1000000          │
    #   # └─────────────────────────┴──────────────────┘
    def nanoseconds
      Utils.wrap_expr(_rbexpr.duration_nanoseconds)
    end

    # Offset this date by a relative time offset.
    #
    # This differs from `Polars.col("foo") + timedelta` in that it can
    # take months and leap years into account. Note that only a single minus
    # sign is allowed in the `by` string, as the first character.
    #
    # @param by [String]
    #   The offset is dictated by the following string language:
    #
    #     - 1ns   (1 nanosecond)
    #     - 1us   (1 microsecond)
    #     - 1ms   (1 millisecond)
    #     - 1s    (1 second)
    #     - 1m    (1 minute)
    #     - 1h    (1 hour)
    #     - 1d    (1 day)
    #     - 1w    (1 week)
    #     - 1mo   (1 calendar month)
    #     - 1y    (1 calendar year)
    #     - 1i    (1 index count)
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dates" => Polars.date_range(
    #         DateTime.new(2000, 1, 1), DateTime.new(2005, 1, 1), "1y"
    #       )
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("dates").dt.offset_by("1y").alias("date_plus_1y"),
    #       Polars.col("dates").dt.offset_by("-1y2mo").alias("date_min")
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ date_plus_1y        ┆ date_min            │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[μs]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 1998-11-01 00:00:00 │
    #   # │ 2002-01-01 00:00:00 ┆ 1999-11-01 00:00:00 │
    #   # │ 2003-01-01 00:00:00 ┆ 2000-11-01 00:00:00 │
    #   # │ 2004-01-01 00:00:00 ┆ 2001-11-01 00:00:00 │
    #   # │ 2005-01-01 00:00:00 ┆ 2002-11-01 00:00:00 │
    #   # │ 2006-01-01 00:00:00 ┆ 2003-11-01 00:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def offset_by(by)
      Utils.wrap_expr(_rbexpr.dt_offset_by(by))
    end

    # Roll backward to the first day of the month.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dates" => Polars.date_range(
    #         DateTime.new(2000, 1, 15, 2),
    #         DateTime.new(2000, 12, 15, 2),
    #         "1mo"
    #       )
    #     }
    #   )
    #   df.select(Polars.col("dates").dt.month_start)
    #   # =>
    #   # shape: (12, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2000-01-01 02:00:00 │
    #   # │ 2000-02-01 02:00:00 │
    #   # │ 2000-03-01 02:00:00 │
    #   # │ 2000-04-01 02:00:00 │
    #   # │ …                   │
    #   # │ 2000-09-01 02:00:00 │
    #   # │ 2000-10-01 02:00:00 │
    #   # │ 2000-11-01 02:00:00 │
    #   # │ 2000-12-01 02:00:00 │
    #   # └─────────────────────┘
    def month_start
      Utils.wrap_expr(_rbexpr.dt_month_start)
    end

    # Roll forward to the last day of the month.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dates" => Polars.date_range(
    #         DateTime.new(2000, 1, 15, 2),
    #         DateTime.new(2000, 12, 15, 2),
    #         "1mo"
    #       )
    #     }
    #   )
    #   df.select(Polars.col("dates").dt.month_end)
    #   # =>
    #   # shape: (12, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2000-01-31 02:00:00 │
    #   # │ 2000-02-29 02:00:00 │
    #   # │ 2000-03-31 02:00:00 │
    #   # │ 2000-04-30 02:00:00 │
    #   # │ …                   │
    #   # │ 2000-09-30 02:00:00 │
    #   # │ 2000-10-31 02:00:00 │
    #   # │ 2000-11-30 02:00:00 │
    #   # │ 2000-12-31 02:00:00 │
    #   # └─────────────────────┘
    def month_end
      Utils.wrap_expr(_rbexpr.dt_month_end)
    end
  end
end

module Polars
  # Namespace for datetime related expressions.
  class DateTimeExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # def truncate
    # end

    # def round
    # end

    #
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-06-30 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-12-27 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2001 │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2001 │
    #   # ├╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-06-30 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2    │
    #   # ├╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-02-01 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2    │
    #   # ├╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-02-01 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 5    │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 9    │
    #   # └──────┘
    def week
      Utils.wrap_expr(_rbexpr.week)
    end

    # Extract the week day from the underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the weekday number where monday = 0 and sunday = 6
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-04 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # │ 0       ┆ 1            ┆ 1           │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3       ┆ 4            ┆ 4           │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 6       ┆ 7            ┆ 7           │
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-04 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # │ 0       ┆ 1            ┆ 1           │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3       ┆ 4            ┆ 4           │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 6       ┆ 7            ┆ 7           │
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-04 00:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # │ 0       ┆ 1            ┆ 1           │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3       ┆ 4            ┆ 4           │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 6       ┆ 7            ┆ 7           │
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-01 12:00:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 12   │
    #   # ├╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-01 00:02:00 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2    │
    #   # ├╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-01 00:00:03.111110 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3    │
    #   # ├╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3.11111  │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2001-01-01 00:00:02 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2    │
    #   # ├╌╌╌╌╌╌┤
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

    def millisecond
      Utils.wrap_expr(_rbexpr.millisecond)
    end

    def microsecond
      Utils.wrap_expr(_rbexpr.microsecond)
    end

    def nanosecond
      Utils.wrap_expr(_rbexpr.nanosecond)
    end

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

    def timestamp(tu = "us")
      Utils.wrap_expr(_rbexpr.timestamp(tu))
    end

    def with_time_unit(tu)
      Utils.wrap_expr(_rbexpr.dt_with_time_unit(tu))
    end

    def cast_time_unit(tu)
      Utils.wrap_expr(_rbexpr.dt_cast_time_unit(tu))
    end

    def with_time_zone(tz)
      Utils.wrap_expr(_rbexpr.dt_with_time_zone(tz))
    end

    def cast_time_zone(tz)
      Utils.wrap_expr(_rbexpr.dt_cast_time_zone(tz))
    end

    def tz_localize(tz)
      Utils.wrap_expr(_rbexpr.dt_tz_localize(tz))
    end

    def days
      Utils.wrap_expr(_rbexpr.duration_days)
    end

    def hours
      Utils.wrap_expr(_rbexpr.duration_hours)
    end

    def minutes
      Utils.wrap_expr(_rbexpr.duration_minutes)
    end

    def seconds
      Utils.wrap_expr(_rbexpr.duration_seconds)
    end

    def milliseconds
      Utils.wrap_expr(_rbexpr.duration_milliseconds)
    end

    def microseconds
      Utils.wrap_expr(_rbexpr.duration_microseconds)
    end

    def nanoseconds
      Utils.wrap_expr(_rbexpr.duration_nanoseconds)
    end

    def offset_by(by)
      Utils.wrap_expr(_rbexpr.dt_offset_by(by))
    end
  end
end

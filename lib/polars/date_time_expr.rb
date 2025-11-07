module Polars
  # Namespace for datetime related expressions.
  class DateTimeExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Offset by `n` business days.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param n
    #   Number of business days to offset by. Can be a single number of an
    #   expression.
    # @param week_mask
    #   Which days of the week to count. The default is Monday to Friday.
    #   If you wanted to count only Monday to Thursday, you would pass
    #   `[true, true, true, true, false, false, false]`.
    # @param roll
    #   What to do when the start date lands on a non-business day. Options are:
    #
    #   - `'raise'`: raise an error
    #   - `'forward'`: move to the next business day
    #   - `'backward'`: move to the previous business day
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"start" => [Date.new(2020, 1, 1), Date.new(2020, 1, 2)]})
    #   df.with_columns(result: Polars.col("start").dt.add_business_days(5))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬────────────┐
    #   # │ start      ┆ result     │
    #   # │ ---        ┆ ---        │
    #   # │ date       ┆ date       │
    #   # ╞════════════╪════════════╡
    #   # │ 2020-01-01 ┆ 2020-01-08 │
    #   # │ 2020-01-02 ┆ 2020-01-09 │
    #   # └────────────┴────────────┘
    def add_business_days(
      n,
      week_mask: [true, true, true, true, true, false, false],
      roll: "raise"
    )
      n_rbexpr = Utils.parse_into_expression(n)
      Utils.wrap_expr(
        _rbexpr.dt_add_business_days(
          n_rbexpr,
          week_mask,
          [],
          roll
        )
      )
    end

    # Divide the date/datetime range into buckets.
    #
    # Each date/datetime is mapped to the start of its bucket.
    #
    # @param every [String]
    #   Every interval start and period length
    #
    # @return [Expr]
    #
    # @note
    #   The `every` argument is created with the
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
    #   df = (
    #     Polars.datetime_range(
    #       DateTime.new(2001, 1, 1),
    #       DateTime.new(2001, 1, 2),
    #       "225m",
    #       eager: true
    #     )
    #     .alias("datetime")
    #     .to_frame
    #   )
    #   # =>
    #   # shape: (7, 1)
    #   # ┌─────────────────────┐
    #   # │ datetime            │
    #   # │ ---                 │
    #   # │ datetime[ns]        │
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
    #   df.select(Polars.col("datetime").dt.truncate("1h"))
    #   # =>
    #   # shape: (7, 1)
    #   # ┌─────────────────────┐
    #   # │ datetime            │
    #   # │ ---                 │
    #   # │ datetime[ns]        │
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
    #   df = (
    #     Polars.datetime_range(
    #       DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 1, 1), "10m", eager: true
    #     )
    #     .alias("datetime")
    #     .to_frame
    #   )
    #   df.select(["datetime", Polars.col("datetime").dt.truncate("30m").alias("truncate")])
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ datetime            ┆ truncate            │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:10:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:20:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:30:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:40:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:50:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 01:00:00 ┆ 2001-01-01 01:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def truncate(every)
      if !every.is_a?(Expr)
        every = Utils.parse_as_duration_string(every)
      end

      every = Utils.parse_into_expression(every, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.dt_truncate(every))
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
    #   df = (
    #     Polars.datetime_range(
    #       DateTime.new(2001, 1, 1),
    #       DateTime.new(2001, 1, 2),
    #       "225m",
    #       eager: true
    #     )
    #     .alias("datetime")
    #     .to_frame
    #   )
    #   df.with_columns(Polars.col("datetime").dt.round("1h").alias("round"))
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ datetime            ┆ round               │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 03:45:00 ┆ 2001-01-01 04:00:00 │
    #   # │ 2001-01-01 07:30:00 ┆ 2001-01-01 08:00:00 │
    #   # │ 2001-01-01 11:15:00 ┆ 2001-01-01 11:00:00 │
    #   # │ 2001-01-01 15:00:00 ┆ 2001-01-01 15:00:00 │
    #   # │ 2001-01-01 18:45:00 ┆ 2001-01-01 19:00:00 │
    #   # │ 2001-01-01 22:30:00 ┆ 2001-01-01 23:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    #
    # @example
    #   df = (
    #     Polars.datetime_range(
    #       DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 1, 1), "10m", eager: true
    #     )
    #     .alias("datetime")
    #     .to_frame
    #   )
    #   df.with_columns(Polars.col("datetime").dt.round("30m").alias("round"))
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ datetime            ┆ round               │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:10:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-01 00:20:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:30:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:40:00 ┆ 2001-01-01 00:30:00 │
    #   # │ 2001-01-01 00:50:00 ┆ 2001-01-01 01:00:00 │
    #   # │ 2001-01-01 01:00:00 ┆ 2001-01-01 01:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def round(every)
      every = Utils.parse_into_expression(every, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.dt_round(every))
    end

    # Replace time unit.
    #
    # @param year [Object]
    #   Column or literal.
    # @param month [Object]
    #   Column or literal, ranging from 1-12.
    # @param day [Object]
    #   Column or literal, ranging from 1-31.
    # @param hour [Object]
    #   Column or literal, ranging from 0-23.
    # @param minute [Object]
    #   Column or literal, ranging from 0-59.
    # @param second [Object]
    #   Column or literal, ranging from 0-59.
    # @param microsecond [Object]
    #   Column or literal, ranging from 0-999999.
    # @param ambiguous [String]
    #   Determine how to deal with ambiguous datetimes:
    #
    #   - `'raise'` (default): raise
    #   - `'earliest'`: use the earliest datetime
    #   - `'latest'`: use the latest datetime
    #   - `'null'`: set to null
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => [Date.new(2024, 4, 1), Date.new(2025, 3, 16)],
    #       "new_day" => [10, 15]
    #     }
    #   )
    #   df.with_columns(Polars.col("date").dt.replace(day: "new_day").alias("replaced"))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────────┬─────────┬────────────┐
    #   # │ date       ┆ new_day ┆ replaced   │
    #   # │ ---        ┆ ---     ┆ ---        │
    #   # │ date       ┆ i64     ┆ date       │
    #   # ╞════════════╪═════════╪════════════╡
    #   # │ 2024-04-01 ┆ 10      ┆ 2024-04-10 │
    #   # │ 2025-03-16 ┆ 15      ┆ 2025-03-15 │
    #   # └────────────┴─────────┴────────────┘
    #
    # @example
    #   df.with_columns(Polars.col("date").dt.replace(year: 1800).alias("replaced"))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────────┬─────────┬────────────┐
    #   # │ date       ┆ new_day ┆ replaced   │
    #   # │ ---        ┆ ---     ┆ ---        │
    #   # │ date       ┆ i64     ┆ date       │
    #   # ╞════════════╪═════════╪════════════╡
    #   # │ 2024-04-01 ┆ 10      ┆ 1800-04-01 │
    #   # │ 2025-03-16 ┆ 15      ┆ 1800-03-16 │
    #   # └────────────┴─────────┴────────────┘
    def replace(
      year: nil,
      month: nil,
      day: nil,
      hour: nil,
      minute: nil,
      second: nil,
      microsecond: nil,
      ambiguous: "raise"
    )
      day, month, year, hour, minute, second, microsecond = (
        Utils.parse_into_list_of_expressions(
          day, month, year, hour, minute, second, microsecond
        )
      )
      ambiguous_expr = Utils.parse_into_expression(ambiguous, str_as_lit: true)
      Utils.wrap_expr(
        _rbexpr.dt_replace(
          year,
          month,
          day,
          hour,
          minute,
          second,
          microsecond,
          ambiguous_expr
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
      time = Utils.parse_into_expression(time)
      Utils.wrap_expr(_rbexpr.dt_combine(time, time_unit))
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [
    #         DateTime.new(2020, 3, 1),
    #         DateTime.new(2020, 4, 1),
    #         DateTime.new(2020, 5, 1)
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime")
    #     .dt.to_string("%Y/%m/%d %H:%M:%S")
    #     .alias("datetime_string")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ datetime            ┆ datetime_string     │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[ns]        ┆ str                 │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2020-03-01 00:00:00 ┆ 2020/03/01 00:00:00 │
    #   # │ 2020-04-01 00:00:00 ┆ 2020/04/01 00:00:00 │
    #   # │ 2020-05-01 00:00:00 ┆ 2020/05/01 00:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def to_string(format)
      Utils.wrap_expr(_rbexpr.dt_to_string(format))
    end

    # Format Date/datetime with a formatting rule.
    #
    # See [chrono strftime/strptime](https://docs.rs/chrono/latest/chrono/format/strftime/index.html).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [
    #         Time.utc(2020, 3, 1),
    #         Time.utc(2020, 4, 1),
    #         Time.utc(2020, 5, 1)
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime")
    #     .dt.strftime("%Y/%m/%d %H:%M:%S")
    #     .alias("datetime_string")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ datetime            ┆ datetime_string     │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[ns]        ┆ str                 │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2020-03-01 00:00:00 ┆ 2020/03/01 00:00:00 │
    #   # │ 2020-04-01 00:00:00 ┆ 2020/04/01 00:00:00 │
    #   # │ 2020-05-01 00:00:00 ┆ 2020/05/01 00:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    #
    # @example If you're interested in the day name / month name, you can use `'%A'` / `'%B'`:
    #   df.with_columns(
    #     day_name: Polars.col("datetime").dt.strftime("%A"),
    #     month_name: Polars.col("datetime").dt.strftime("%B")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────────────────┬───────────┬────────────┐
    #   # │ datetime            ┆ day_name  ┆ month_name │
    #   # │ ---                 ┆ ---       ┆ ---        │
    #   # │ datetime[ns]        ┆ str       ┆ str        │
    #   # ╞═════════════════════╪═══════════╪════════════╡
    #   # │ 2020-03-01 00:00:00 ┆ Sunday    ┆ March      │
    #   # │ 2020-04-01 00:00:00 ┆ Wednesday ┆ April      │
    #   # │ 2020-05-01 00:00:00 ┆ Friday    ┆ May        │
    #   # └─────────────────────┴───────────┴────────────┘
    def strftime(fmt)
      Utils.wrap_expr(_rbexpr.strftime(fmt))
    end

    # Extract the millennium from underlying representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the millennium number in the calendar date.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => [
    #         Date.new(999, 12, 31),
    #         Date.new(1897, 5, 7),
    #         Date.new(2000, 1, 1),
    #         Date.new(2001, 7, 5),
    #         Date.new(3002, 10, 20)
    #       ]
    #     }
    #   )
    #   df.with_columns(mlnm: Polars.col("date").dt.millennium)
    #   # =>
    #   # shape: (5, 2)
    #   # ┌────────────┬──────┐
    #   # │ date       ┆ mlnm │
    #   # │ ---        ┆ ---  │
    #   # │ date       ┆ i32  │
    #   # ╞════════════╪══════╡
    #   # │ 0999-12-31 ┆ 1    │
    #   # │ 1897-05-07 ┆ 2    │
    #   # │ 2000-01-01 ┆ 2    │
    #   # │ 2001-07-05 ┆ 3    │
    #   # │ 3002-10-20 ┆ 4    │
    #   # └────────────┴──────┘
    def millennium
      Utils.wrap_expr(_rbexpr.dt_millennium)
    end

    # Extract the century from underlying representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the century number in the calendar date.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => [
    #         Date.new(999, 12, 31),
    #         Date.new(1897, 5, 7),
    #         Date.new(2000, 1, 1),
    #         Date.new(2001, 7, 5),
    #         Date.new(3002, 10, 20)
    #       ]
    #     }
    #   )
    #   df.with_columns(cent: Polars.col("date").dt.century)
    #   # =>
    #   # shape: (5, 2)
    #   # ┌────────────┬──────┐
    #   # │ date       ┆ cent │
    #   # │ ---        ┆ ---  │
    #   # │ date       ┆ i32  │
    #   # ╞════════════╪══════╡
    #   # │ 0999-12-31 ┆ 10   │
    #   # │ 1897-05-07 ┆ 19   │
    #   # │ 2000-01-01 ┆ 20   │
    #   # │ 2001-07-05 ┆ 21   │
    #   # │ 3002-10-20 ┆ 31   │
    #   # └────────────┴──────┘
    def century
      Utils.wrap_expr(_rbexpr.dt_century)
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
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(1977, 1, 1), Date.new(1978, 1, 1), Date.new(1979, 1, 1)]}
    #   )
    #   df.with_columns(
    #     calendar_year: Polars.col("date").dt.year,
    #     iso_year: Polars.col("date").dt.iso_year
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬───────────────┬──────────┐
    #   # │ date       ┆ calendar_year ┆ iso_year │
    #   # │ ---        ┆ ---           ┆ ---      │
    #   # │ date       ┆ i32           ┆ i32      │
    #   # ╞════════════╪═══════════════╪══════════╡
    #   # │ 1977-01-01 ┆ 1977          ┆ 1976     │
    #   # │ 1978-01-01 ┆ 1978          ┆ 1977     │
    #   # │ 1979-01-01 ┆ 1979          ┆ 1979     │
    #   # └────────────┴───────────────┴──────────┘
    def year
      Utils.wrap_expr(_rbexpr.dt_year)
    end

    # Determine whether each day lands on a business day.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param week_mask [Array]
    #   Which days of the week to count. The default is Monday to Friday.
    #   If you wanted to count only Monday to Thursday, you would pass
    #   `[true, true, true, true, false, false, false]`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"start" => [Date.new(2020, 1, 3), Date.new(2020, 1, 5)]})
    #   df.with_columns(is_business_day: Polars.col("start").dt.is_business_day)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬─────────────────┐
    #   # │ start      ┆ is_business_day │
    #   # │ ---        ┆ ---             │
    #   # │ date       ┆ bool            │
    #   # ╞════════════╪═════════════════╡
    #   # │ 2020-01-03 ┆ true            │
    #   # │ 2020-01-05 ┆ false           │
    #   # └────────────┴─────────────────┘
    def is_business_day(
      week_mask: [true, true, true, true, true, false, false]
    )
      Utils.wrap_expr(
        _rbexpr.dt_is_business_day(
          week_mask,
          []
        )
      )
    end

    # Determine whether the year of the underlying date is a leap year.
    #
    # Applies to Date and Datetime columns.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(2000, 1, 1), Date.new(2001, 1, 1), Date.new(2002, 1, 1)]}
    #   )
    #   df.with_columns(
    #     leap_year: Polars.col("date").dt.is_leap_year
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬───────────┐
    #   # │ date       ┆ leap_year │
    #   # │ ---        ┆ ---       │
    #   # │ date       ┆ bool      │
    #   # ╞════════════╪═══════════╡
    #   # │ 2000-01-01 ┆ true      │
    #   # │ 2001-01-01 ┆ false     │
    #   # │ 2002-01-01 ┆ false     │
    #   # └────────────┴───────────┘
    def is_leap_year
      Utils.wrap_expr(_rbexpr.dt_is_leap_year)
    end

    # Extract ISO year from underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the year number in the ISO standard.
    # This may not correspond with the calendar year.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(1977, 1, 1), Date.new(1978, 1, 1), Date.new(1979, 1, 1)]}
    #   )
    #   df.select(
    #     "date",
    #     Polars.col("date").dt.year.alias("calendar_year"),
    #     Polars.col("date").dt.iso_year.alias("iso_year")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬───────────────┬──────────┐
    #   # │ date       ┆ calendar_year ┆ iso_year │
    #   # │ ---        ┆ ---           ┆ ---      │
    #   # │ date       ┆ i32           ┆ i32      │
    #   # ╞════════════╪═══════════════╪══════════╡
    #   # │ 1977-01-01 ┆ 1977          ┆ 1976     │
    #   # │ 1978-01-01 ┆ 1978          ┆ 1977     │
    #   # │ 1979-01-01 ┆ 1979          ┆ 1979     │
    #   # └────────────┴───────────────┴──────────┘
    def iso_year
      Utils.wrap_expr(_rbexpr.dt_iso_year)
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
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(2001, 1, 1), Date.new(2001, 6, 30), Date.new(2001, 12, 27)]}
    #   )
    #   df.with_columns(Polars.col("date").dt.quarter.alias("quarter"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬─────────┐
    #   # │ date       ┆ quarter │
    #   # │ ---        ┆ ---     │
    #   # │ date       ┆ i8      │
    #   # ╞════════════╪═════════╡
    #   # │ 2001-01-01 ┆ 1       │
    #   # │ 2001-06-30 ┆ 2       │
    #   # │ 2001-12-27 ┆ 4       │
    #   # └────────────┴─────────┘
    def quarter
      Utils.wrap_expr(_rbexpr.dt_quarter)
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
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(2001, 1, 1), Date.new(2001, 6, 30), Date.new(2001, 12, 27)]}
    #   )
    #   df.with_columns(Polars.col("date").dt.month.alias("month"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬───────┐
    #   # │ date       ┆ month │
    #   # │ ---        ┆ ---   │
    #   # │ date       ┆ i8    │
    #   # ╞════════════╪═══════╡
    #   # │ 2001-01-01 ┆ 1     │
    #   # │ 2001-06-30 ┆ 6     │
    #   # │ 2001-12-27 ┆ 12    │
    #   # └────────────┴───────┘
    def month
      Utils.wrap_expr(_rbexpr.dt_month)
    end

    # Extract the number of days in the month from the underlying Date representation.
    #
    # Applies to Date and Datetime columns.
    #
    # Returns the number of days in the month.
    # The return value ranges from 28 to 31.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(2001, 1, 1), Date.new(2001, 2, 1), Date.new(2000, 2, 1)]}
    #   )
    #   df.with_columns(Polars.col("date").dt.days_in_month.alias("days_in_month"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬───────────────┐
    #   # │ date       ┆ days_in_month │
    #   # │ ---        ┆ ---           │
    #   # │ date       ┆ i8            │
    #   # ╞════════════╪═══════════════╡
    #   # │ 2001-01-01 ┆ 31            │
    #   # │ 2001-02-01 ┆ 28            │
    #   # │ 2000-02-01 ┆ 29            │
    #   # └────────────┴───────────────┘
    def days_in_month
      Utils.wrap_expr(_rbexpr.dt_days_in_month)
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
    #   df = Polars::DataFrame.new(
    #     {"date" => [Date.new(2001, 1, 1), Date.new(2001, 6, 30), Date.new(2001, 12, 27)]}
    #   )
    #   df.with_columns(Polars.col("date").dt.week.alias("week"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬──────┐
    #   # │ date       ┆ week │
    #   # │ ---        ┆ ---  │
    #   # │ date       ┆ i8   │
    #   # ╞════════════╪══════╡
    #   # │ 2001-01-01 ┆ 1    │
    #   # │ 2001-06-30 ┆ 26   │
    #   # │ 2001-12-27 ┆ 52   │
    #   # └────────────┴──────┘
    def week
      Utils.wrap_expr(_rbexpr.dt_week)
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         Date.new(2001, 12, 22), Date.new(2001, 12, 25), eager: true
    #       )
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("date").dt.weekday.alias("weekday"),
    #     Polars.col("date").dt.day.alias("day_of_month"),
    #     Polars.col("date").dt.ordinal_day.alias("day_of_year")
    #   )
    #   # =>
    #   # shape: (4, 4)
    #   # ┌────────────┬─────────┬──────────────┬─────────────┐
    #   # │ date       ┆ weekday ┆ day_of_month ┆ day_of_year │
    #   # │ ---        ┆ ---     ┆ ---          ┆ ---         │
    #   # │ date       ┆ i8      ┆ i8           ┆ i16         │
    #   # ╞════════════╪═════════╪══════════════╪═════════════╡
    #   # │ 2001-12-22 ┆ 6       ┆ 22           ┆ 356         │
    #   # │ 2001-12-23 ┆ 7       ┆ 23           ┆ 357         │
    #   # │ 2001-12-24 ┆ 1       ┆ 24           ┆ 358         │
    #   # │ 2001-12-25 ┆ 2       ┆ 25           ┆ 359         │
    #   # └────────────┴─────────┴──────────────┴─────────────┘
    def weekday
      Utils.wrap_expr(_rbexpr.dt_weekday)
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         Date.new(2001, 12, 22), Date.new(2001, 12, 25), eager: true
    #       )
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("date").dt.weekday.alias("weekday"),
    #     Polars.col("date").dt.day.alias("day_of_month"),
    #     Polars.col("date").dt.ordinal_day.alias("day_of_year")
    #   )
    #   # =>
    #   # shape: (4, 4)
    #   # ┌────────────┬─────────┬──────────────┬─────────────┐
    #   # │ date       ┆ weekday ┆ day_of_month ┆ day_of_year │
    #   # │ ---        ┆ ---     ┆ ---          ┆ ---         │
    #   # │ date       ┆ i8      ┆ i8           ┆ i16         │
    #   # ╞════════════╪═════════╪══════════════╪═════════════╡
    #   # │ 2001-12-22 ┆ 6       ┆ 22           ┆ 356         │
    #   # │ 2001-12-23 ┆ 7       ┆ 23           ┆ 357         │
    #   # │ 2001-12-24 ┆ 1       ┆ 24           ┆ 358         │
    #   # │ 2001-12-25 ┆ 2       ┆ 25           ┆ 359         │
    #   # └────────────┴─────────┴──────────────┴─────────────┘
    def day
      Utils.wrap_expr(_rbexpr.dt_day)
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         Date.new(2001, 12, 22), Date.new(2001, 12, 25), eager: true
    #       )
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("date").dt.weekday.alias("weekday"),
    #     Polars.col("date").dt.day.alias("day_of_month"),
    #     Polars.col("date").dt.ordinal_day.alias("day_of_year")
    #   )
    #   # =>
    #   # shape: (4, 4)
    #   # ┌────────────┬─────────┬──────────────┬─────────────┐
    #   # │ date       ┆ weekday ┆ day_of_month ┆ day_of_year │
    #   # │ ---        ┆ ---     ┆ ---          ┆ ---         │
    #   # │ date       ┆ i8      ┆ i8           ┆ i16         │
    #   # ╞════════════╪═════════╪══════════════╪═════════════╡
    #   # │ 2001-12-22 ┆ 6       ┆ 22           ┆ 356         │
    #   # │ 2001-12-23 ┆ 7       ┆ 23           ┆ 357         │
    #   # │ 2001-12-24 ┆ 1       ┆ 24           ┆ 358         │
    #   # │ 2001-12-25 ┆ 2       ┆ 25           ┆ 359         │
    #   # └────────────┴─────────┴──────────────┴─────────────┘
    def ordinal_day
      Utils.wrap_expr(_rbexpr.dt_ordinal_day)
    end

    # Time
    #
    # @return [Expr]
    def time
      Utils.wrap_expr(_rbexpr.dt_time)
    end

    # Date
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000)
    #       ]
    #     }
    #   )
    #   df.with_columns(Polars.col("datetime").dt.date.alias("date"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────────┬────────────┐
    #   # │ datetime                ┆ date       │
    #   # │ ---                     ┆ ---        │
    #   # │ datetime[ns]            ┆ date       │
    #   # ╞═════════════════════════╪════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1978-01-01 │
    #   # │ 2024-10-13 05:30:14.500 ┆ 2024-10-13 │
    #   # │ 2065-01-01 10:20:30.060 ┆ 2065-01-01 │
    #   # └─────────────────────────┴────────────┘
    def date
      Utils.wrap_expr(_rbexpr.dt_date)
    end

    # Datetime
    #
    # @return [Expr]
    def datetime
      Utils.wrap_expr(_rbexpr.dt_datetime)
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000)
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second.alias("second"),
    #     Polars.col("datetime").dt.millisecond.alias("millisecond")
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────────────────────────┬──────┬────────┬────────┬─────────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second ┆ millisecond │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    ┆ ---         │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ i8     ┆ i32         │
    #   # ╞═════════════════════════╪══════╪════════╪════════╪═════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1      ┆ 0           │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14     ┆ 500         │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30     ┆ 60          │
    #   # └─────────────────────────┴──────┴────────┴────────┴─────────────┘
    def hour
      Utils.wrap_expr(_rbexpr.dt_hour)
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000)
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second.alias("second"),
    #     Polars.col("datetime").dt.millisecond.alias("millisecond")
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────────────────────────┬──────┬────────┬────────┬─────────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second ┆ millisecond │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    ┆ ---         │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ i8     ┆ i32         │
    #   # ╞═════════════════════════╪══════╪════════╪════════╪═════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1      ┆ 0           │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14     ┆ 500         │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30     ┆ 60          │
    #   # └─────────────────────────┴──────┴────────┴────────┴─────────────┘
    def minute
      Utils.wrap_expr(_rbexpr.dt_minute)
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
    #       "datetime" => [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000)
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second.alias("second"),
    #     Polars.col("datetime").dt.millisecond.alias("millisecond")
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────────────────────────┬──────┬────────┬────────┬─────────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second ┆ millisecond │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    ┆ ---         │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ i8     ┆ i32         │
    #   # ╞═════════════════════════╪══════╪════════╪════════╪═════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1      ┆ 0           │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14     ┆ 500         │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30     ┆ 60          │
    #   # └─────────────────────────┴──────┴────────┴────────┴─────────────┘
    #
    # @example
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second(fractional: true).alias("second")
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────────────────────────┬──────┬────────┬────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ f64    │
    #   # ╞═════════════════════════╪══════╪════════╪════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1.0    │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14.5   │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30.06  │
    #   # └─────────────────────────┴──────┴────────┴────────┘
    def second(fractional: false)
      sec = Utils.wrap_expr(_rbexpr.dt_second)
      if fractional
        sec + (Utils.wrap_expr(_rbexpr.dt_nanosecond) / F.lit(1_000_000_000.0))
      else
        sec
      end
    end

    # Extract milliseconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime": [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000),
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second.alias("second"),
    #     Polars.col("datetime").dt.millisecond.alias("millisecond")
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────────────────────────┬──────┬────────┬────────┬─────────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second ┆ millisecond │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    ┆ ---         │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ i8     ┆ i32         │
    #   # ╞═════════════════════════╪══════╪════════╪════════╪═════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1      ┆ 0           │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14     ┆ 500         │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30     ┆ 60          │
    #   # └─────────────────────────┴──────┴────────┴────────┴─────────────┘
    def millisecond
      Utils.wrap_expr(_rbexpr.dt_millisecond)
    end

    # Extract microseconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime": [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000),
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second.alias("second"),
    #     Polars.col("datetime").dt.microsecond.alias("microsecond")
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────────────────────────┬──────┬────────┬────────┬─────────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second ┆ microsecond │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    ┆ ---         │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ i8     ┆ i32         │
    #   # ╞═════════════════════════╪══════╪════════╪════════╪═════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1      ┆ 0           │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14     ┆ 500000      │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30     ┆ 60000       │
    #   # └─────────────────────────┴──────┴────────┴────────┴─────────────┘
    def microsecond
      Utils.wrap_expr(_rbexpr.dt_microsecond)
    end

    # Extract nanoseconds from underlying DateTime representation.
    #
    # Applies to Datetime columns.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime": [
    #         Time.utc(1978, 1, 1, 1, 1, 1, 0),
    #         Time.utc(2024, 10, 13, 5, 30, 14, 500_000),
    #         Time.utc(2065, 1, 1, 10, 20, 30, 60_000),
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("datetime").dt.hour.alias("hour"),
    #     Polars.col("datetime").dt.minute.alias("minute"),
    #     Polars.col("datetime").dt.second.alias("second"),
    #     Polars.col("datetime").dt.nanosecond.alias("nanosecond")
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────────────────────────┬──────┬────────┬────────┬────────────┐
    #   # │ datetime                ┆ hour ┆ minute ┆ second ┆ nanosecond │
    #   # │ ---                     ┆ ---  ┆ ---    ┆ ---    ┆ ---        │
    #   # │ datetime[ns]            ┆ i8   ┆ i8     ┆ i8     ┆ i32        │
    #   # ╞═════════════════════════╪══════╪════════╪════════╪════════════╡
    #   # │ 1978-01-01 01:01:01     ┆ 1    ┆ 1      ┆ 1      ┆ 0          │
    #   # │ 2024-10-13 05:30:14.500 ┆ 5    ┆ 30     ┆ 14     ┆ 500000000  │
    #   # │ 2065-01-01 10:20:30.060 ┆ 10   ┆ 20     ┆ 30     ┆ 60000000   │
    #   # └─────────────────────────┴──────┴────────┴────────┴────────────┘
    def nanosecond
      Utils.wrap_expr(_rbexpr.dt_nanosecond)
    end

    # Get the time passed since the Unix EPOCH in the give time unit.
    #
    # @param time_unit ["us", "ns", "ms", "s", "d"]
    #   Time unit.
    #
    # @return [Expr]
    #
    # @example
    #   df = (
    #     Polars.date_range(Date.new(2001, 1, 1), Date.new(2001, 1, 3), eager: true)
    #     .alias("date")
    #     .to_frame
    #   )
    #   df.with_columns(
    #     Polars.col("date").dt.epoch.alias("epoch_ns"),
    #     Polars.col("date").dt.epoch("s").alias("epoch_s")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬─────────────────┬───────────┐
    #   # │ date       ┆ epoch_ns        ┆ epoch_s   │
    #   # │ ---        ┆ ---             ┆ ---       │
    #   # │ date       ┆ i64             ┆ i64       │
    #   # ╞════════════╪═════════════════╪═══════════╡
    #   # │ 2001-01-01 ┆ 978307200000000 ┆ 978307200 │
    #   # │ 2001-01-02 ┆ 978393600000000 ┆ 978393600 │
    #   # │ 2001-01-03 ┆ 978480000000000 ┆ 978480000 │
    #   # └────────────┴─────────────────┴───────────┘
    def epoch(time_unit = "us")
      if Utils::DTYPE_TEMPORAL_UNITS.include?(time_unit)
        timestamp(time_unit)
      elsif time_unit == "s"
        timestamp("ms").floordiv(F.lit(1000, dtype: Int64))
      elsif time_unit == "d"
        Utils.wrap_expr(_rbexpr).cast(:date).cast(:i32)
      else
        raise ArgumentError, "time_unit must be one of {'ns', 'us', 'ms', 's', 'd'}, got #{time_unit.inspect}"
      end
    end

    # Return a timestamp in the given time unit.
    #
    # @param time_unit ["us", "ns", "ms"]
    #   Time unit.
    #
    # @return [Expr]
    #
    # @example
    #   df = (
    #     Polars.date_range(Date.new(2001, 1, 1), Date.new(2001, 1, 3), eager: true)
    #     .alias("date")
    #     .to_frame
    #   )
    #   df.with_columns(
    #     Polars.col("date").dt.timestamp.alias("timestamp_us"),
    #     Polars.col("date").dt.timestamp("ms").alias("timestamp_ms")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬─────────────────┬──────────────┐
    #   # │ date       ┆ timestamp_us    ┆ timestamp_ms │
    #   # │ ---        ┆ ---             ┆ ---          │
    #   # │ date       ┆ i64             ┆ i64          │
    #   # ╞════════════╪═════════════════╪══════════════╡
    #   # │ 2001-01-01 ┆ 978307200000000 ┆ 978307200000 │
    #   # │ 2001-01-02 ┆ 978393600000000 ┆ 978393600000 │
    #   # │ 2001-01-03 ┆ 978480000000000 ┆ 978480000000 │
    #   # └────────────┴─────────────────┴──────────────┘
    def timestamp(time_unit = "us")
      Utils.wrap_expr(_rbexpr.dt_timestamp(time_unit))
    end

    # Set time unit of a Series of dtype Datetime or Duration.
    #
    # This does not modify underlying data, and should be used to fix an incorrect
    # time unit.
    #
    # @param time_unit ["ns", "us", "ms"]
    #   Time unit for the `Datetime` Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         Time.utc(2001, 1, 1),
    #         Time.utc(2001, 1, 3),
    #         "1d",
    #         time_unit: "ns",
    #         eager: true
    #       )
    #     }
    #   )
    #   df.select(
    #     Polars.col("date"),
    #     Polars.col("date").dt.with_time_unit("us").alias("time_unit_us")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────┬───────────────────────┐
    #   # │ date                ┆ time_unit_us          │
    #   # │ ---                 ┆ ---                   │
    #   # │ datetime[ns]        ┆ datetime[μs]          │
    #   # ╞═════════════════════╪═══════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ +32971-04-28 00:00:00 │
    #   # │ 2001-01-02 00:00:00 ┆ +32974-01-22 00:00:00 │
    #   # │ 2001-01-03 00:00:00 ┆ +32976-10-18 00:00:00 │
    #   # └─────────────────────┴───────────────────────┘
    def with_time_unit(time_unit)
      Utils.wrap_expr(_rbexpr.dt_with_time_unit(time_unit))
    end

    # Cast the underlying data to another time unit. This may lose precision.
    #
    # @param time_unit ["ns", "us", "ms"]
    #   Time unit for the `Datetime` Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2001, 1, 1), DateTime.new(2001, 1, 3), "1d", eager: true
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
    #   # │ datetime[ns]        ┆ datetime[ms]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 ┆ 2001-01-01 00:00:00 │
    #   # │ 2001-01-02 00:00:00 ┆ 2001-01-02 00:00:00 ┆ 2001-01-02 00:00:00 │
    #   # │ 2001-01-03 00:00:00 ┆ 2001-01-03 00:00:00 ┆ 2001-01-03 00:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┘
    def cast_time_unit(time_unit)
      Utils.wrap_expr(_rbexpr.dt_cast_time_unit(time_unit))
    end

    # Set time zone for a Series of type Datetime.
    #
    # @param time_zone [String]
    #   Time zone for the `Datetime` Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 3, 1),
    #         DateTime.new(2020, 5, 1),
    #         "1mo",
    #         time_zone: "UTC",
    #         eager: true
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
    #   # │ datetime[ns, UTC]       ┆ datetime[ns, Europe/London] │
    #   # ╞═════════════════════════╪═════════════════════════════╡
    #   # │ 2020-03-01 00:00:00 UTC ┆ 2020-03-01 00:00:00 GMT     │
    #   # │ 2020-04-01 00:00:00 UTC ┆ 2020-04-01 01:00:00 BST     │
    #   # │ 2020-05-01 00:00:00 UTC ┆ 2020-05-01 01:00:00 BST     │
    #   # └─────────────────────────┴─────────────────────────────┘
    def convert_time_zone(time_zone)
      Utils.wrap_expr(_rbexpr.dt_convert_time_zone(time_zone))
    end

    # Cast time zone for a Series of type Datetime.
    #
    # Different from `convert_time_zone`, this will also modify
    # the underlying timestamp,
    #
    # @param time_zone [String]
    #     Time zone for the `Datetime` Series. Pass `nil` to unset time zone.
    # @param ambiguous [String]
    #     Determine how to deal with ambiguous datetimes.
    # @param non_existent [String]
    #     Determine how to deal with non-existent datetimes.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "london_timezone": Polars.datetime_range(
    #         Time.utc(2020, 3, 1),
    #         Time.utc(2020, 7, 1),
    #         "1mo",
    #         time_zone: "UTC",
    #         eager: true,
    #       ).dt.convert_time_zone("Europe/London")
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("london_timezone"),
    #       Polars.col("london_timezone")
    #       .dt.replace_time_zone("Europe/Amsterdam")
    #       .alias("London_to_Amsterdam")
    #     ]
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────────────────────────┬────────────────────────────────┐
    #   # │ london_timezone             ┆ London_to_Amsterdam            │
    #   # │ ---                         ┆ ---                            │
    #   # │ datetime[ns, Europe/London] ┆ datetime[ns, Europe/Amsterdam] │
    #   # ╞═════════════════════════════╪════════════════════════════════╡
    #   # │ 2020-03-01 00:00:00 GMT     ┆ 2020-03-01 00:00:00 CET        │
    #   # │ 2020-04-01 01:00:00 BST     ┆ 2020-04-01 01:00:00 CEST       │
    #   # │ 2020-05-01 01:00:00 BST     ┆ 2020-05-01 01:00:00 CEST       │
    #   # │ 2020-06-01 01:00:00 BST     ┆ 2020-06-01 01:00:00 CEST       │
    #   # │ 2020-07-01 01:00:00 BST     ┆ 2020-07-01 01:00:00 CEST       │
    #   # └─────────────────────────────┴────────────────────────────────┘
    #
    # @example You can use `ambiguous` to deal with ambiguous datetimes:
    #   dates = [
    #     "2018-10-28 01:30",
    #     "2018-10-28 02:00",
    #     "2018-10-28 02:30",
    #     "2018-10-28 02:00"
    #   ]
    #   df = Polars::DataFrame.new(
    #     {
    #       "ts" => Polars::Series.new(dates).str.strptime(Polars::Datetime),
    #       "ambiguous" => ["earliest", "earliest", "latest", "latest"]
    #     }
    #   )
    #   df.with_columns(
    #     ts_localized: Polars.col("ts").dt.replace_time_zone(
    #       "Europe/Brussels", ambiguous: Polars.col("ambiguous")
    #     )
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬───────────┬───────────────────────────────┐
    #   # │ ts                  ┆ ambiguous ┆ ts_localized                  │
    #   # │ ---                 ┆ ---       ┆ ---                           │
    #   # │ datetime[μs]        ┆ str       ┆ datetime[μs, Europe/Brussels] │
    #   # ╞═════════════════════╪═══════════╪═══════════════════════════════╡
    #   # │ 2018-10-28 01:30:00 ┆ earliest  ┆ 2018-10-28 01:30:00 CEST      │
    #   # │ 2018-10-28 02:00:00 ┆ earliest  ┆ 2018-10-28 02:00:00 CEST      │
    #   # │ 2018-10-28 02:30:00 ┆ latest    ┆ 2018-10-28 02:30:00 CET       │
    #   # │ 2018-10-28 02:00:00 ┆ latest    ┆ 2018-10-28 02:00:00 CET       │
    #   # └─────────────────────┴───────────┴───────────────────────────────┘
    def replace_time_zone(time_zone, ambiguous: "raise", non_existent: "raise")
      unless ambiguous.is_a?(Expr)
        ambiguous = Polars.lit(ambiguous)
      end
      Utils.wrap_expr(_rbexpr.dt_replace_time_zone(time_zone, ambiguous._rbexpr, non_existent))
    end

    # Extract the days from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 3, 1), DateTime.new(2020, 5, 1), "1mo", eager: true
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
    #   # │ datetime[ns]        ┆ i64       │
    #   # ╞═════════════════════╪═══════════╡
    #   # │ 2020-03-01 00:00:00 ┆ null      │
    #   # │ 2020-04-01 00:00:00 ┆ 31        │
    #   # │ 2020-05-01 00:00:00 ┆ 30        │
    #   # └─────────────────────┴───────────┘
    def total_days(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_days(fractional))
    end
    alias_method :days, :total_days

    # Extract the hours from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 4), "1d", eager: true
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
    #   # │ datetime[ns]        ┆ i64        │
    #   # ╞═════════════════════╪════════════╡
    #   # │ 2020-01-01 00:00:00 ┆ null       │
    #   # │ 2020-01-02 00:00:00 ┆ 24         │
    #   # │ 2020-01-03 00:00:00 ┆ 24         │
    #   # │ 2020-01-04 00:00:00 ┆ 24         │
    #   # └─────────────────────┴────────────┘
    def total_hours(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_hours(fractional))
    end
    alias_method :hours, :total_hours

    # Extract the minutes from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 4), "1d", eager: true
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
    #   # │ datetime[ns]        ┆ i64          │
    #   # ╞═════════════════════╪══════════════╡
    #   # │ 2020-01-01 00:00:00 ┆ null         │
    #   # │ 2020-01-02 00:00:00 ┆ 1440         │
    #   # │ 2020-01-03 00:00:00 ┆ 1440         │
    #   # │ 2020-01-04 00:00:00 ┆ 1440         │
    #   # └─────────────────────┴──────────────┘
    def total_minutes(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_minutes(fractional))
    end
    alias_method :minutes, :total_minutes

    # Extract the seconds from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 4, 0), "1m", eager: true
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
    #   # │ datetime[ns]        ┆ i64          │
    #   # ╞═════════════════════╪══════════════╡
    #   # │ 2020-01-01 00:00:00 ┆ null         │
    #   # │ 2020-01-01 00:01:00 ┆ 60           │
    #   # │ 2020-01-01 00:02:00 ┆ 60           │
    #   # │ 2020-01-01 00:03:00 ┆ 60           │
    #   # │ 2020-01-01 00:04:00 ┆ 60           │
    #   # └─────────────────────┴──────────────┘
    def total_seconds(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_seconds(fractional))
    end
    alias_method :seconds, :total_seconds

    # Extract the milliseconds from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1), "1ms", eager: true
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
    #   # │ datetime[ns]            ┆ i64               │
    #   # ╞═════════════════════════╪═══════════════════╡
    #   # │ 2020-01-01 00:00:00     ┆ null              │
    #   # │ 2020-01-01 00:00:00.001 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.002 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.003 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.004 ┆ 1                 │
    #   # │ …                       ┆ …                 │
    #   # │ 2020-01-01 00:00:00.996 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.997 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.998 ┆ 1                 │
    #   # │ 2020-01-01 00:00:00.999 ┆ 1                 │
    #   # │ 2020-01-01 00:00:01     ┆ 1                 │
    #   # └─────────────────────────┴───────────────────┘
    def total_milliseconds(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_milliseconds(fractional))
    end
    alias_method :milliseconds, :total_milliseconds

    # Extract the microseconds from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1), "1ms", eager: true
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
    #   # │ datetime[ns]            ┆ i64               │
    #   # ╞═════════════════════════╪═══════════════════╡
    #   # │ 2020-01-01 00:00:00     ┆ null              │
    #   # │ 2020-01-01 00:00:00.001 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.002 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.003 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.004 ┆ 1000              │
    #   # │ …                       ┆ …                 │
    #   # │ 2020-01-01 00:00:00.996 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.997 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.998 ┆ 1000              │
    #   # │ 2020-01-01 00:00:00.999 ┆ 1000              │
    #   # │ 2020-01-01 00:00:01     ┆ 1000              │
    #   # └─────────────────────────┴───────────────────┘
    def total_microseconds(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_microseconds(fractional))
    end
    alias_method :microseconds, :total_microseconds

    # Extract the nanoseconds from a Duration type.
    #
    # @param fractional [Boolean]
    #   Whether to include the fractional component of the second.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "date" => Polars.datetime_range(
    #         DateTime.new(2020, 1, 1), DateTime.new(2020, 1, 1, 0, 0, 1), "1ms", eager: true
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
    #   # │ datetime[ns]            ┆ i64              │
    #   # ╞═════════════════════════╪══════════════════╡
    #   # │ 2020-01-01 00:00:00     ┆ null             │
    #   # │ 2020-01-01 00:00:00.001 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.002 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.003 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.004 ┆ 1000000          │
    #   # │ …                       ┆ …                │
    #   # │ 2020-01-01 00:00:00.996 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.997 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.998 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:00.999 ┆ 1000000          │
    #   # │ 2020-01-01 00:00:01     ┆ 1000000          │
    #   # └─────────────────────────┴──────────────────┘
    def total_nanoseconds(fractional: false)
      Utils.wrap_expr(_rbexpr.dt_total_nanoseconds(fractional))
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
    #       "dates" => Polars.datetime_range(
    #         DateTime.new(2000, 1, 1), DateTime.new(2005, 1, 1), "1y", eager: true
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
    #   # │ datetime[ns]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2001-01-01 00:00:00 ┆ 1998-11-01 00:00:00 │
    #   # │ 2002-01-01 00:00:00 ┆ 1999-11-01 00:00:00 │
    #   # │ 2003-01-01 00:00:00 ┆ 2000-11-01 00:00:00 │
    #   # │ 2004-01-01 00:00:00 ┆ 2001-11-01 00:00:00 │
    #   # │ 2005-01-01 00:00:00 ┆ 2002-11-01 00:00:00 │
    #   # │ 2006-01-01 00:00:00 ┆ 2003-11-01 00:00:00 │
    #   # └─────────────────────┴─────────────────────┘
    def offset_by(by)
      by = Utils.parse_into_expression(by, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.dt_offset_by(by))
    end

    # Roll backward to the first day of the month.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dates" => Polars.datetime_range(
    #         DateTime.new(2000, 1, 15, 2),
    #         DateTime.new(2000, 12, 15, 2),
    #         "1mo",
    #         eager: true
    #       )
    #     }
    #   )
    #   df.select(Polars.col("dates").dt.month_start)
    #   # =>
    #   # shape: (12, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[ns]        │
    #   # ╞═════════════════════╡
    #   # │ 2000-01-01 02:00:00 │
    #   # │ 2000-02-01 02:00:00 │
    #   # │ 2000-03-01 02:00:00 │
    #   # │ 2000-04-01 02:00:00 │
    #   # │ 2000-05-01 02:00:00 │
    #   # │ …                   │
    #   # │ 2000-08-01 02:00:00 │
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
    #       "dates" => Polars.datetime_range(
    #         DateTime.new(2000, 1, 15, 2),
    #         DateTime.new(2000, 12, 15, 2),
    #         "1mo",
    #         eager: true
    #       )
    #     }
    #   )
    #   df.select(Polars.col("dates").dt.month_end)
    #   # =>
    #   # shape: (12, 1)
    #   # ┌─────────────────────┐
    #   # │ dates               │
    #   # │ ---                 │
    #   # │ datetime[ns]        │
    #   # ╞═════════════════════╡
    #   # │ 2000-01-31 02:00:00 │
    #   # │ 2000-02-29 02:00:00 │
    #   # │ 2000-03-31 02:00:00 │
    #   # │ 2000-04-30 02:00:00 │
    #   # │ 2000-05-31 02:00:00 │
    #   # │ …                   │
    #   # │ 2000-08-31 02:00:00 │
    #   # │ 2000-09-30 02:00:00 │
    #   # │ 2000-10-31 02:00:00 │
    #   # │ 2000-11-30 02:00:00 │
    #   # │ 2000-12-31 02:00:00 │
    #   # └─────────────────────┘
    def month_end
      Utils.wrap_expr(_rbexpr.dt_month_end)
    end

    # Base offset from UTC.
    #
    # This is usually constant for all datetimes in a given time zone, but
    # may vary in the rare case that a country switches time zone, like
    # Samoa (Apia) did at the end of 2011.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "ts" => [DateTime.new(2011, 12, 29), DateTime.new(2012, 1, 1)],
    #     }
    #   )
    #   df = df.with_columns(Polars.col("ts").dt.replace_time_zone("Pacific/Apia"))
    #   df.with_columns(Polars.col("ts").dt.base_utc_offset.alias("base_utc_offset"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────────────────────┬─────────────────┐
    #   # │ ts                         ┆ base_utc_offset │
    #   # │ ---                        ┆ ---             │
    #   # │ datetime[ns, Pacific/Apia] ┆ duration[ms]    │
    #   # ╞════════════════════════════╪═════════════════╡
    #   # │ 2011-12-29 00:00:00 -10    ┆ -11h            │
    #   # │ 2012-01-01 00:00:00 +14    ┆ 13h             │
    #   # └────────────────────────────┴─────────────────┘
    def base_utc_offset
      Utils.wrap_expr(_rbexpr.dt_base_utc_offset)
    end

    # Additional offset currently in effect (typically due to daylight saving time).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "ts" => [DateTime.new(2020, 10, 25), DateTime.new(2020, 10, 26)],
    #     }
    #   )
    #   df = df.with_columns(Polars.col("ts").dt.replace_time_zone("Europe/London"))
    #   df.with_columns(Polars.col("ts").dt.dst_offset.alias("dst_offset"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────────────────────────────┬──────────────┐
    #   # │ ts                          ┆ dst_offset   │
    #   # │ ---                         ┆ ---          │
    #   # │ datetime[ns, Europe/London] ┆ duration[ms] │
    #   # ╞═════════════════════════════╪══════════════╡
    #   # │ 2020-10-25 00:00:00 BST     ┆ 1h           │
    #   # │ 2020-10-26 00:00:00 GMT     ┆ 0ms          │
    #   # └─────────────────────────────┴──────────────┘
    def dst_offset
      Utils.wrap_expr(_rbexpr.dt_dst_offset)
    end
  end
end

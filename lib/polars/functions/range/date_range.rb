module Polars
  module Functions
    # Create a range of type `Datetime` (or `Date`).
    #
    # @param start [Object]
    #   Lower bound of the date range.
    # @param stop [Object]
    #   Upper bound of the date range.
    # @param interval [Object]
    #   Interval periods. It can be a polars duration string, such as `3d12h4m25s`
    #   representing 3 days, 12 hours, 4 minutes, and 25 seconds.
    # @param closed ["both", "left", "right", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @note
    #   If both `low` and `high` are passed as date types (not datetime), and the
    #   interval granularity is no finer than 1d, the returned range is also of
    #   type date. All other permutations return a datetime Series.
    #
    # @example Using polars duration string to specify the interval
    #   Polars.date_range(Date.new(2022, 1, 1), Date.new(2022, 3, 1), "1mo", eager: true).alias(
    #     "date"
    #   )
    #   # =>
    #   # shape: (3,)
    #   # Series: 'date' [date]
    #   # [
    #   #         2022-01-01
    #   #         2022-02-01
    #   #         2022-03-01
    #   # ]
    def date_range(
      start,
      stop,
      interval = "1d",
      closed: "both",
      eager: false
    )
      interval = Utils.parse_interval_argument(interval)

      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)

      result = Utils.wrap_expr(
        Plr.date_range(start_rbexpr, end_rbexpr, interval, closed)
      )

      if eager
        return F.select(result).to_series
      end

      result
    end

    # Create a column of date ranges.
    #
    # @param start [Object]
    #   Lower bound of the date range.
    # @param stop [Object]
    #   Upper bound of the date range.
    # @param interval [Object]
    #   Interval of the range periods, specified using the Polars duration string language (see "Notes" section below).
    # @param closed ["both", "left", "right", "none"]
    #   Define which sides of the range are closed (inclusive).
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @note
    #   `interval` is created according to the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   Or combine them:
    #   "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    #   By "calendar day", we mean the corresponding time on the next day (which may
    #   not be 24 hours, due to daylight savings). Similarly for "calendar week",
    #   "calendar month", "calendar quarter", and "calendar year".
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "start" => [Date.new(2022, 1, 1), Date.new(2022, 1, 2)],
    #       "end" => Date.new(2022, 1, 3)
    #     }
    #   )
    #   df.with_columns(date_range: Polars.date_ranges("start", "end"))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────────┬────────────┬─────────────────────────────────┐
    #   # │ start      ┆ end        ┆ date_range                      │
    #   # │ ---        ┆ ---        ┆ ---                             │
    #   # │ date       ┆ date       ┆ list[date]                      │
    #   # ╞════════════╪════════════╪═════════════════════════════════╡
    #   # │ 2022-01-01 ┆ 2022-01-03 ┆ [2022-01-01, 2022-01-02, 2022-… │
    #   # │ 2022-01-02 ┆ 2022-01-03 ┆ [2022-01-02, 2022-01-03]        │
    #   # └────────────┴────────────┴─────────────────────────────────┘
    def date_ranges(
      start,
      stop,
      interval = "1d",
      closed: "both",
      eager: false
    )
      interval = Utils.parse_interval_argument(interval)
      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)

      result = Utils.wrap_expr(Plr.date_ranges(start_rbexpr, end_rbexpr, interval, closed))

      if eager
        return F.select(result).to_series
      end

      result
    end
  end
end

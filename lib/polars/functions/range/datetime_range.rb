module Polars
  module Functions
    # Generate a datetime range.
    #
    # @param start [Object]
    #   Lower bound of the datetime range.
    # @param stop [Object]
    #   Upper bound of the datetime range.
    # @param interval [String]
    #   Interval of the range periods, specified using the Polars duration string language.
    # @param closed ['both', 'left', 'right', 'none']
    #   Define which sides of the range are closed (inclusive).
    # @param time_unit [nil, 'ns', 'us', 'ms']
    #   Time unit of the resulting `Datetime` data type.
    # @param time_zone [String]
    #   Time zone of the resulting `Datetime` data type.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @example Using Polars duration string to specify the interval:
    #   Polars.datetime_range(
    #     DateTime.new(2022, 1, 1), DateTime.new(2022, 3, 1), "1mo", eager: true
    #   ).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns]]
    #   # [
    #   #         2022-01-01 00:00:00
    #   #         2022-02-01 00:00:00
    #   #         2022-03-01 00:00:00
    #   # ]
    #
    # @example Specifying a time zone:
    #   Polars.datetime_range(
    #     DateTime.new(2022, 1, 1),
    #     DateTime.new(2022, 3, 1),
    #     "1mo",
    #     time_zone: "America/New_York",
    #     eager: true
    #   ).alias("datetime")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'datetime' [datetime[ns, America/New_York]]
    #   # [
    #   #         2022-01-01 00:00:00 EST
    #   #         2022-02-01 00:00:00 EST
    #   #         2022-03-01 00:00:00 EST
    #   # ]
    def datetime_range(
      start,
      stop,
      interval = "1d",
      closed: "both",
      time_unit: nil,
      time_zone: nil,
      eager: false
    )
      interval = Utils.parse_interval_argument(interval)
      if time_unit.nil? && interval.include?("ns")
        time_unit = "ns"
      end

      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)
      result = Utils.wrap_expr(
        Plr.datetime_range(
          start_rbexpr, end_rbexpr, interval, closed, time_unit, time_zone
        )
      )

      if eager
        return Polars.select(result).to_series
      end

      result
    end

    # Create a column of datetime ranges.
    #
    # @param start [Object]
    #   Lower bound of the datetime range.
    # @param stop [Object]
    #   Upper bound of the datetime range.
    # @param interval [String]
    #   Interval of the range periods, specified using the Polars duration string language.
    # @param closed ['both', 'left', 'right', 'none']
    #   Define which sides of the range are closed (inclusive).
    # @param time_unit [nil, 'ns', 'us', 'ms']
    #   Time unit of the resulting `Datetime` data type.
    # @param time_zone [String]
    #   Time zone of the resulting `Datetime` data type.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "start" => [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2)],
    #       "end" => DateTime.new(2022, 1, 3),
    #     }
    #   )
    #   df.select(datetime_range: Polars.datetime_ranges("start", "end"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────────────────┐
    #   # │ datetime_range                  │
    #   # │ ---                             │
    #   # │ list[datetime[ns]]              │
    #   # ╞═════════════════════════════════╡
    #   # │ [2022-01-01 00:00:00, 2022-01-… │
    #   # │ [2022-01-02 00:00:00, 2022-01-… │
    #   # └─────────────────────────────────┘
    def datetime_ranges(
      start,
      stop,
      interval: "1d",
      closed: "both",
      time_unit: nil,
      time_zone: nil,
      eager: false
    )
      interval = Utils.parse_interval_argument(interval)
      if time_unit.nil? && interval.include?("ns")
        time_unit = "ns"
      end

      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)

      result = Utils.wrap_expr(
        Plr.datetime_ranges(
          start_rbexpr, end_rbexpr, interval, closed, time_unit, time_zone
        )
      )

      if eager
        return Polars.select(result).to_series
      end

      result
    end
  end
end

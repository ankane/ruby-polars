module Polars
  module Functions
    # Generate a time range.
    #
    # @param start [Object]
    #   Lower bound of the time range.
    # @param stop [Object]
    #   Upper bound of the time range.
    # @param interval [String]
    #   Interval of the range periods, specified using the Polars duration string language.
    # @param closed ['both', 'left', 'right', 'none']
    #   Define which sides of the range are closed (inclusive).
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `False` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @example
    #   Polars.time_range(
    #     Time.utc(2000, 1, 1, 14, 0),
    #     nil,
    #     "3h15m",
    #     eager: true
    #   ).alias("time")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'time' [time]
    #   # [
    #   #         14:00:00
    #   #         17:15:00
    #   #         20:30:00
    #   #         23:45:00
    #   # ]
    def time_range(
      start = nil,
      stop = nil,
      interval = "1h",
      closed: "both",
      eager: false
    )
      interval = Utils.parse_interval_argument(interval)
      ["y", "mo", "w", "d"].each do |unit|
        if interval.include?(unit)
          msg = "invalid interval unit for time_range: found #{unit.inspect}"
          raise ArgumentError, msg
        end
      end

      if start.nil?
        # date part is ignored
        start = ::Time.utc(2000, 1, 1, 0, 0, 0)
      end
      if stop.nil?
        # date part is ignored
        stop = ::Time.utc(2000, 1, 1, 23, 59, 59, 999999)
      end

      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)

      result = Utils.wrap_expr(Plr.time_range(start_rbexpr, end_rbexpr, interval, closed))

      if eager
        return Polars.select(result).to_series
      end

      result
    end

    # Create a column of time ranges.
    #
    # @param start [Object]
    #   Lower bound of the time range.
    # @param stop [Object]
    #   Upper bound of the time range.
    # @param interval [Integer]
    #   Interval of the range periods, specified using the Polars duration string language.
    # @param closed ['both', 'left', 'right', 'none']
    #   Define which sides of the range are closed (inclusive).
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "start" => [Time.utc(2000, 1, 1, 9, 0), Time.utc(2000, 1, 1, 10, 0)],
    #       "end" => Time.utc(2000, 1, 1, 11, 0)
    #     }
    #   )
    #   df.select(time_range: Polars.time_ranges("start", "end"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────────────────────────────┐
    #   # │ time_range                     │
    #   # │ ---                            │
    #   # │ list[time]                     │
    #   # ╞════════════════════════════════╡
    #   # │ [09:00:00, 10:00:00, 11:00:00] │
    #   # │ [10:00:00, 11:00:00]           │
    #   # └────────────────────────────────┘
    def time_ranges(
      start = nil,
      stop = nil,
      interval = "1h",
      closed: "both",
      eager: false
    )
      interval = Utils.parse_interval_argument(interval)
      ["y", "mo", "w", "d"].each do |unit|
        if interval.include?(unit)
          msg = "invalid interval unit for time_range: found #{unit.inspect}"
          raise ArgumentError, msg
        end
      end

      if start.nil?
        # date part is ignored
        start = ::Time.utc(2000, 1, 1, 0, 0, 0)
      end
      if stop.nil?
        # date part is ignored
        stop = ::Time.utc(2000, 1, 1, 23, 59, 59, 999999)
      end

      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)

      result = Utils.wrap_expr(Plr.time_ranges(start_rbexpr, end_rbexpr, interval, closed))

      if eager
        return Polars.select(result).to_series
      end

      result
    end
  end
end

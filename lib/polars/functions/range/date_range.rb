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
    # @param lazy [Boolean]
    #   Return an expression.
    # @param closed ["both", "left", "right", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param name [String]
    #   Name of the output Series.
    # @param time_unit [nil, "ns", "us", "ms"]
    #   Set the time unit.
    # @param time_zone [String]
    #   Optional timezone
    #
    # @return [Object]
    #
    # @note
    #   If both `low` and `high` are passed as date types (not datetime), and the
    #   interval granularity is no finer than 1d, the returned range is also of
    #   type date. All other permutations return a datetime Series.
    #
    # @example Using polars duration string to specify the interval
    #   Polars.date_range(Date.new(2022, 1, 1), Date.new(2022, 3, 1), "1mo", name: "drange")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'drange' [date]
    #   # [
    #   #         2022-01-01
    #   #         2022-02-01
    #   #         2022-03-01
    #   # ]
    #
    # @example Using `timedelta` object to specify the interval:
    #   Polars.date_range(
    #       DateTime.new(1985, 1, 1),
    #       DateTime.new(1985, 1, 10),
    #       "1d12h",
    #       time_unit: "ms"
    #   )
    #   # =>
    #   # shape: (7,)
    #   # Series: '' [datetime[ms]]
    #   # [
    #   #         1985-01-01 00:00:00
    #   #         1985-01-02 12:00:00
    #   #         1985-01-04 00:00:00
    #   #         1985-01-05 12:00:00
    #   #         1985-01-07 00:00:00
    #   #         1985-01-08 12:00:00
    #   #         1985-01-10 00:00:00
    #   # ]
    def date_range(
      start,
      stop,
      interval,
      lazy: false,
      closed: "both",
      name: nil,
      time_unit: nil,
      time_zone: nil
    )
      if defined?(ActiveSupport::Duration) && interval.is_a?(ActiveSupport::Duration)
        raise Todo
      else
        interval = interval.to_s
        if interval.include?(" ")
          interval = interval.gsub(" ", "")
        end
      end

      if time_unit.nil?
        if interval.include?("ns")
          time_unit = "ns"
        else
          time_unit = "us"
        end
      end

      start_rbexpr = Utils.parse_as_expression(start)
      stop_rbexpr = Utils.parse_as_expression(stop)

      result = Utils.wrap_expr(
        Plr.date_range(start_rbexpr, stop_rbexpr, interval, closed, time_unit, time_zone)
      )

      result = result.alias(name.to_s)

      if !lazy
        return select(result).to_series
      end

      result
    end
  end
end

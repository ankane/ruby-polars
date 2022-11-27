module Polars
  module Functions
    # Convert categorical variables into dummy/indicator variables.
    #
    # @param df [DataFrame]
    #   DataFrame to convert.
    # @param columns [Array, nil]
    #   A subset of columns to convert to dummy variables. `nil` means
    #   "all columns".
    #
    # @return [DataFrame]
    def get_dummies(df, columns: nil)
      df.to_dummies(columns: columns)
    end

    def concat(items, rechunk: true, how: "vertical", parallel: true)
      if items.empty?
        raise ArgumentError, "cannot concat empty list"
      end

      first = items[0]
      if first.is_a?(DataFrame)
        if how == "vertical"
          out = Utils.wrap_df(_concat_df(items))
        elsif how == "diagonal"
          out = Utils.wrap_df(_diag_concat_df(items))
        elsif how == "horizontal"
          out = Utils.wrap_df(_hor_concat_df(items))
        else
          raise ArgumentError, "how must be one of {{'vertical', 'diagonal', 'horizontal'}}, got #{how}"
        end
      elsif first.is_a?(LazyFrame)
        if how == "vertical"
          # TODO
          return Utils.wrap_ldf(_concat_lf(items, rechunk, parallel))
        else
          raise ArgumentError, "Lazy only allows 'vertical' concat strategy."
        end
      elsif first.is_a?(Series)
        # TODO
        out = Utils.wrap_s(_concat_series(items))
      elsif first.is_a?(Expr)
        out = first
        items[1..-1].each do |e|
          out = out.append(e)
        end
      else
        raise ArgumentError, "did not expect type: #{first.class.name} in 'Polars.concat'."
      end

      if rechunk
        out.rechunk
      else
        out
      end
    end

    # Create a range of type `Datetime` (or `Date`).
    #
    # @param low [Object]
    #   Lower bound of the date range.
    # @param high [Object]
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
      low,
      high,
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

      if low.is_a?(Expr) || high.is_a?(Expr) || lazy
        low = Utils.expr_to_lit_or_expr(low, str_to_lit: true)
        high = Utils.expr_to_lit_or_expr(high, str_to_lit: true)
        return Utils.wrap_expr(
          _rb_date_range_lazy(low, high, interval, closed, name, time_zone)
        )
      end

      low, low_is_date = _ensure_datetime(low)
      high, high_is_date = _ensure_datetime(high)

      if !time_unit.nil?
        tu = time_unit
      else
        tu = "us"
      end

      start = Utils._datetime_to_pl_timestamp(low, tu)
      stop = Utils._datetime_to_pl_timestamp(high, tu)
      if name.nil?
        name = ""
      end

      dt_range = Utils.wrap_s(
        _rb_date_range(start, stop, interval, closed, name, tu, time_zone)
      )
      if low_is_date && high_is_date && !["h", "m", "s"].any? { |v| _interval_granularity(interval).end_with?(v) }
        dt_range = dt_range.cast(Date)
      end

      dt_range
    end

    # def cut
    # end

    # def align_frames
    # end

    # Return a new Series of given length and type, filled with ones.
    #
    # @param n [Integer]
    #   Number of elements in the `Series`
    # @param dtype [Symbol]
    #   DataType of the elements, defaults to `:f64`
    #
    # @return [Series]
    #
    # @note
    #   In the lazy API you should probably not use this, but use `lit(1)`
    #   instead.
    def ones(n, dtype: nil)
      s = Series.new([1.0])
      if dtype
        s = s.cast(dtype)
      end
      s.new_from_index(0, n)
    end

    # Return a new Series of given length and type, filled with zeros.
    #
    # @param n [Integer]
    #   Number of elements in the `Series`
    # @param dtype [Symbol]
    #   DataType of the elements, defaults to `:f64`
    #
    # @return [Series]
    #
    # @note
    #   In the lazy API you should probably not use this, but use `lit(0)`
    #   instead.
    def zeros(n, dtype: nil)
      s = Series.new([0.0])
      if dtype
        s = s.cast(dtype)
      end
      s.new_from_index(0, n)
    end

    private

    def _ensure_datetime(value)
      is_date_type = false
      if !value.is_a?(DateTime)
        value = DateTime.new(value.year, value.month, value.day)
        is_date_type = true
      end
      [value, is_date_type]
    end

    # TODO
    def _interval_granularity(interval)
      interval
    end
  end
end

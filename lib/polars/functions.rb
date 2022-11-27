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
      # if low_is_date && high_is_date && !_interval_granularity(interval).endswith(("h", "m", "s"))
      #   dt_range = dt_range.cast(Date)
      # end

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
  end
end

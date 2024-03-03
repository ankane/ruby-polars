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

    # Aggregate multiple Dataframes/Series to a single DataFrame/Series.
    #
    # @param items [Object]
    #   DataFrames/Series/LazyFrames to concatenate.
    # @param rechunk [Boolean]
    #   Make sure that all data is in contiguous memory.
    # @param how ["vertical", "vertical_relaxed", "diagonal", "horizontal"]
    #   LazyFrames do not support the `horizontal` strategy.
    #
    #   - Vertical: applies multiple `vstack` operations.
    #   - Diagonal: finds a union between the column schemas and fills missing column values with null.
    #   - Horizontal: stacks Series horizontally and fills with nulls if the lengths don't match.
    # @param parallel [Boolean]
    #   Only relevant for LazyFrames. This determines if the concatenated
    #   lazy computations may be executed in parallel.
    #
    # @return [Object]
    #
    # @example
    #   df1 = Polars::DataFrame.new({"a" => [1], "b" => [3]})
    #   df2 = Polars::DataFrame.new({"a" => [2], "b" => [4]})
    #   Polars.concat([df1, df2])
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 3   │
    #   # │ 2   ┆ 4   │
    #   # └─────┴─────┘
    def concat(items, rechunk: true, how: "vertical", parallel: true)
      if items.empty?
        raise ArgumentError, "cannot concat empty list"
      end

      first = items[0]
      if first.is_a?(DataFrame)
        if how == "vertical"
          out = Utils.wrap_df(Plr.concat_df(items))
        elsif how == "diagonal"
          out = Utils.wrap_df(Plr.concat_df_diagonal(items))
        elsif how == "horizontal"
          out = Utils.wrap_df(Plr.concat_df_horizontal(items))
        else
          raise ArgumentError, "how must be one of {{'vertical', 'diagonal', 'horizontal'}}, got #{how}"
        end
      elsif first.is_a?(LazyFrame)
        if how == "vertical"
          return Utils.wrap_ldf(Plr.concat_lf(items, rechunk, parallel, false))
        elsif how == "vertical_relaxed"
          return Utils.wrap_ldf(Plr.concat_lf(items, rechunk, parallel, true))
        elsif how == "diagonal"
          return Utils.wrap_ldf(Plr.concat_lf_diagonal(items, rechunk, parallel, false))
        else
          raise ArgumentError, "Lazy only allows 'vertical', 'vertical_relaxed', and 'diagonal' concat strategy."
        end
      elsif first.is_a?(Series)
        # TODO
        out = Utils.wrap_s(Plr.concat_series(items))
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
        _rb_date_range(start_rbexpr, stop_rbexpr, interval, closed, time_unit, time_zone)
      )

      result = result.alias(name.to_s)

      if !lazy
        return select(result).to_series
      end

      result
    end

    # Bin values into discrete values.
    #
    # @param s [Series]
    #   Series to bin.
    # @param bins [Array]
    #   Bins to create.
    # @param labels [Array]
    #   Labels to assign to the bins. If given the length of labels must be
    #   len(bins) + 1.
    # @param break_point_label [String]
    #   Name given to the breakpoint column.
    # @param category_label [String]
    #   Name given to the category column.
    #
    # @return [DataFrame]
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @example
    #   a = Polars::Series.new("a", 13.times.map { |i| (-30 + i * 5) / 10.0 })
    #   Polars.cut(a, [-1, 1])
    #   # =>
    #   # shape: (12, 3)
    #   # ┌──────┬─────────────┬──────────────┐
    #   # │ a    ┆ break_point ┆ category     │
    #   # │ ---  ┆ ---         ┆ ---          │
    #   # │ f64  ┆ f64         ┆ cat          │
    #   # ╞══════╪═════════════╪══════════════╡
    #   # │ -3.0 ┆ -1.0        ┆ (-inf, -1.0] │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ -2.5 ┆ -1.0        ┆ (-inf, -1.0] │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ -2.0 ┆ -1.0        ┆ (-inf, -1.0] │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ -1.5 ┆ -1.0        ┆ (-inf, -1.0] │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ ...  ┆ ...         ┆ ...          │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.0  ┆ 1.0         ┆ (-1.0, 1.0]  │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.5  ┆ inf         ┆ (1.0, inf]   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2.0  ┆ inf         ┆ (1.0, inf]   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2.5  ┆ inf         ┆ (1.0, inf]   │
    #   # └──────┴─────────────┴──────────────┘
    # def cut(
    #   s,
    #   bins,
    #   labels: nil,
    #   break_point_label: "break_point",
    #   category_label: "category"
    # )
    #   var_nm = s.name

    #   cuts_df = DataFrame.new(
    #     [
    #       Series.new(
    #         break_point_label, bins, dtype: :f64
    #       ).extend_constant(Float::INFINITY, 1)
    #     ]
    #   )

    #   if labels
    #     if labels.length != bins.length + 1
    #       raise ArgumentError, "expected more labels"
    #     end
    #     cuts_df = cuts_df.with_column(Series.new(category_label, labels))
    #   else
    #     cuts_df = cuts_df.with_column(
    #       Polars.format(
    #         "({}, {}]",
    #         Polars.col(break_point_label).shift_and_fill(1, -Float::INFINITY),
    #         Polars.col(break_point_label)
    #       ).alias(category_label)
    #     )
    #   end

    #   cuts_df = cuts_df.with_column(Polars.col(category_label).cast(:cat))

    #   s.cast(:f64)
    #     .sort
    #     .to_frame
    #     .join_asof(
    #       cuts_df,
    #       left_on: var_nm,
    #       right_on: break_point_label,
    #       strategy: "forward"
    #     )
    # end

    # Align a sequence of frames using the uique values from one or more columns as a key.
    #
    # Frames that do not contain the given key values have rows injected (with nulls
    # filling the non-key columns), and each resulting frame is sorted by the key.
    #
    # The original column order of input frames is not changed unless ``select`` is
    # specified (in which case the final column order is determined from that).
    #
    # Note that this does not result in a joined frame - you receive the same number
    # of frames back that you passed in, but each is now aligned by key and has
    # the same number of rows.
    #
    # @param frames [Array]
    #   Sequence of DataFrames or LazyFrames.
    # @param on [Object]
    #   One or more columns whose unique values will be used to align the frames.
    # @param select [Object]
    #   Optional post-alignment column select to constrain and/or order
    #   the columns returned from the newly aligned frames.
    # @param reverse [Object]
    #   Sort the alignment column values in descending order; can be a single
    #   boolean or a list of booleans associated with each column in `on`.
    #
    # @return [Object]
    #
    # @example
    #   df1 = Polars::DataFrame.new(
    #     {
    #       "dt" => [Date.new(2022, 9, 1), Date.new(2022, 9, 2), Date.new(2022, 9, 3)],
    #       "x" => [3.5, 4.0, 1.0],
    #       "y" => [10.0, 2.5, 1.5]
    #     }
    #   )
    #   df2 = Polars::DataFrame.new(
    #     {
    #       "dt" => [Date.new(2022, 9, 2), Date.new(2022, 9, 3), Date.new(2022, 9, 1)],
    #       "x" => [8.0, 1.0, 3.5],
    #       "y" => [1.5, 12.0, 5.0]
    #     }
    #   )
    #   df3 = Polars::DataFrame.new(
    #     {
    #       "dt" => [Date.new(2022, 9, 3), Date.new(2022, 9, 2)],
    #       "x" => [2.0, 5.0],
    #       "y" => [2.5, 2.0]
    #     }
    #   )
    #   af1, af2, af3 = Polars.align_frames(
    #     df1, df2, df3, on: "dt", select: ["x", "y"]
    #   )
    #   (af1 * af2 * af3).fill_null(0).select(Polars.sum(Polars.col("*")).alias("dot"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ dot   │
    #   # │ ---   │
    #   # │ f64   │
    #   # ╞═══════╡
    #   # │ 0.0   │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 167.5 │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 47.0  │
    #   # └───────┘
    def align_frames(
      *frames,
      on:,
      select: nil,
      reverse: false
    )
      if frames.empty?
        return []
      elsif frames.map(&:class).uniq.length != 1
        raise TypeError, "Input frames must be of a consistent type (all LazyFrame or all DataFrame)"
      end

      # establish the superset of all "on" column values, sort, and cache
      eager = frames[0].is_a?(DataFrame)
      alignment_frame = (
        concat(frames.map { |df| df.lazy.select(on) })
          .unique(maintain_order: false)
          .sort(on, reverse: reverse)
      )
      alignment_frame = (
        eager ? alignment_frame.collect.lazy : alignment_frame.cache
      )
      # finally, align all frames
      aligned_frames =
        frames.map do |df|
          alignment_frame.join(
            df.lazy,
            on: alignment_frame.columns,
            how: "left"
          ).select(df.columns)
        end
      if !select.nil?
        aligned_frames = aligned_frames.map { |df| df.select(select) }
      end

      eager ? aligned_frames.map(&:collect) : aligned_frames
    end

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
      if !value.is_a?(::DateTime)
        value = ::DateTime.new(value.year, value.month, value.day)
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

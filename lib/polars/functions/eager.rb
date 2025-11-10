module Polars
  module Functions
    # Aggregate multiple Dataframes/Series to a single DataFrame/Series.
    #
    # @param items [Object]
    #   DataFrames/Series/LazyFrames to concatenate.
    # @param rechunk [Boolean]
    #   Make sure that all data is in contiguous memory.
    # @param how ["vertical", "vertical_relaxed", "diagonal", "diagonal_relaxed", "horizontal"]
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
    #   Polars.concat([df1, df2])  # default is 'vertical' strategy
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
    #
    # @example
    #   df1 = Polars::DataFrame.new({"a" => [1], "b" => [3]})
    #   df2 = Polars::DataFrame.new({"a" => [2.5], "b" => [4]})
    #   Polars.concat([df1, df2], how: "vertical_relaxed")  # 'a' coerced into f64
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1.0 ┆ 3   │
    #   # │ 2.5 ┆ 4   │
    #   # └─────┴─────┘
    #
    # @example
    #   df_h1 = Polars::DataFrame.new({"l1" => [1, 2], "l2" => [3, 4]})
    #   df_h2 = Polars::DataFrame.new({"r1" => [5, 6], "r2" => [7, 8], "r3" => [9, 10]})
    #   Polars.concat([df_h1, df_h2], how: "horizontal")
    #   # =>
    #   # shape: (2, 5)
    #   # ┌─────┬─────┬─────┬─────┬─────┐
    #   # │ l1  ┆ l2  ┆ r1  ┆ r2  ┆ r3  │
    #   # │ --- ┆ --- ┆ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╪═════╪═════╡
    #   # │ 1   ┆ 3   ┆ 5   ┆ 7   ┆ 9   │
    #   # │ 2   ┆ 4   ┆ 6   ┆ 8   ┆ 10  │
    #   # └─────┴─────┴─────┴─────┴─────┘
    #
    # @example
    #   df_d1 = Polars::DataFrame.new({"a" => [1], "b" => [3]})
    #   df_d2 = Polars::DataFrame.new({"a" => [2], "c" => [4]})
    #   Polars.concat([df_d1, df_d2], how: "diagonal")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬──────┬──────┐
    #   # │ a   ┆ b    ┆ c    │
    #   # │ --- ┆ ---  ┆ ---  │
    #   # │ i64 ┆ i64  ┆ i64  │
    #   # ╞═════╪══════╪══════╡
    #   # │ 1   ┆ 3    ┆ null │
    #   # │ 2   ┆ null ┆ 4    │
    #   # └─────┴──────┴──────┘
    #
    # @example
    #   df_a1 = Polars::DataFrame.new({"id" => [1, 2], "x" => [3, 4]})
    #   df_a2 = Polars::DataFrame.new({"id" => [2, 3], "y" => [5, 6]})
    #   df_a3 = Polars::DataFrame.new({"id" => [1, 3], "z" => [7, 8]})
    #   Polars.concat([df_a1, df_a2, df_a3], how: "align")
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬──────┬──────┐
    #   # │ id  ┆ x    ┆ y    ┆ z    │
    #   # │ --- ┆ ---  ┆ ---  ┆ ---  │
    #   # │ i64 ┆ i64  ┆ i64  ┆ i64  │
    #   # ╞═════╪══════╪══════╪══════╡
    #   # │ 1   ┆ 3    ┆ null ┆ 7    │
    #   # │ 2   ┆ 4    ┆ 5    ┆ null │
    #   # │ 3   ┆ null ┆ 6    ┆ 8    │
    #   # └─────┴──────┴──────┴──────┘
    def concat(items, rechunk: true, how: "vertical", parallel: true)
      elems = items.to_a

      if elems.empty?
        raise ArgumentError, "cannot concat empty list"
      end

      if how == "align"
        if !elems[0].is_a?(DataFrame) && !elems[0].is_a?(LazyFrame)
          msg = "'align' strategy is not supported for #{elems[0].class.name}"
          raise TypeError, msg
        end

        # establish common columns, maintaining the order in which they appear
        all_columns = elems.flat_map { |e| e.collect_schema.names }
        key = all_columns.uniq.map.with_index.to_h
        common_cols = elems.map { |e| e.collect_schema.names }
          .reduce { |x, y| Set.new(x) & Set.new(y) }
          .sort_by { |k| key[k] }
        # we require at least one key column for 'align'
        if common_cols.empty?
          msg = "'align' strategy requires at least one common column"
          raise InvalidOperationError, msg
        end

        # align the frame data using a full outer join with no suffix-resolution
        # (so we raise an error in case of column collision, like "horizontal")
        lf = elems.map { |df| df.lazy }.reduce do |x, y|
          x.join(
            y,
            how: "full",
            on: common_cols,
            suffix: "_PL_CONCAT_RIGHT",
            maintain_order: "right_left"
          )
          # Coalesce full outer join columns
          .with_columns(
            common_cols.map { |name| F.coalesce([name, "#{name}_PL_CONCAT_RIGHT"]) }
          )
          .drop(common_cols.map { |name| "#{name}_PL_CONCAT_RIGHT" })
        end.sort(common_cols)

        eager = elems[0].is_a?(DataFrame)
        return eager ? lf.collect : lf
      end

      first = elems[0]

      if first.is_a?(DataFrame)
        if how == "vertical"
          out = Utils.wrap_df(Plr.concat_df(elems))
        elsif how == "vertical_relaxed"
          out = Utils.wrap_ldf(
            Plr.concat_lf(
              elems.map { |df| df.lazy },
              rechunk,
              parallel,
              true
            )
          ).collect(optimizations: QueryOptFlags._eager)
        elsif how == "diagonal"
          out = Utils.wrap_df(Plr.concat_df_diagonal(elems))
        elsif how == "diagonal_relaxed"
          out = Utils.wrap_ldf(
            Plr.concat_lf_diagonal(
              elems.map { |df| df.lazy },
              rechunk,
              parallel,
              true
            )
          ).collect(optimizations: QueryOptFlags._eager)
        elsif how == "horizontal"
          out = Utils.wrap_df(Plr.concat_df_horizontal(elems))
        else
          raise ArgumentError, "how must be one of {{'vertical', 'vertical_relaxed', 'diagonal', 'diagonal_relaxed', 'horizontal'}}, got #{how}"
        end
      elsif first.is_a?(LazyFrame)
        if how == "vertical"
          return Utils.wrap_ldf(Plr.concat_lf(elems, rechunk, parallel, false))
        elsif how == "vertical_relaxed"
          return Utils.wrap_ldf(Plr.concat_lf(elems, rechunk, parallel, true))
        elsif how == "diagonal"
          return Utils.wrap_ldf(Plr.concat_lf_diagonal(elems, rechunk, parallel, false))
        elsif how == "diagonal_relaxed"
          return Utils.wrap_ldf(Plr.concat_lf_diagonal(elems, rechunk, parallel, true))
        elsif how == "horizontal"
          return Utils.wrap_ldf(Plr.concat_lf_horizontal(elems, parallel))
        else
          raise ArgumentError, "Lazy only allows 'vertical', 'vertical_relaxed', 'diagonal', and 'diagonal_relaxed' concat strategy."
        end
      elsif first.is_a?(Series)
        if how == "vertical"
          out = Utils.wrap_s(Plr.concat_series(elems))
        else
          msg = "Series only supports 'vertical' concat strategy"
          raise ArgumentError, msg
        end
      elsif first.is_a?(Expr)
        out = first
        elems[1..-1].each do |e|
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

    # Align a sequence of frames using the unique values from one or more columns as a key.
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
    # @param descending [Object]
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
    #   (af1 * af2 * af3).fill_null(0).select(Polars.sum_horizontal("*").alias("dot"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ dot   │
    #   # │ ---   │
    #   # │ f64   │
    #   # ╞═══════╡
    #   # │ 0.0   │
    #   # │ 167.5 │
    #   # │ 47.0  │
    #   # └───────┘
    def align_frames(
      *frames,
      on:,
      select: nil,
      descending: false
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
          .sort(on, descending: descending)
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
  end
end

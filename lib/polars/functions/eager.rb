module Polars
  module Functions
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
  end
end

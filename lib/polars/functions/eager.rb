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
  end
end

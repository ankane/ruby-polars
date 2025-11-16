module Polars
  # Created by `df.lazy.group_by("foo")`.
  class LazyGroupBy
    # @private
    def initialize(lgb)
      @lgb = lgb
    end

    # Compute aggregations for each group of a group by operation.
    #
    # @param aggs [Array]
    #   Aggregations to compute for each group of the group by operation,
    #   specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names.
    # @param named_aggs [Hash]
    #   Additional aggregations, specified as keyword arguments.
    #   The resulting columns will be renamed to the keyword used.
    #
    # @return [LazyFrame]
    #
    # @example Compute the aggregation of the columns for each group.
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "c"],
    #       "b" => [1, 2, 1, 3, 3],
    #       "c" => [5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   ldf.group_by("a").agg(
    #     [Polars.col("b"), Polars.col("c")]
    #   ).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬───────────┬───────────┐
    #   # │ a   ┆ b         ┆ c         │
    #   # │ --- ┆ ---       ┆ ---       │
    #   # │ str ┆ list[i64] ┆ list[i64] │
    #   # ╞═════╪═══════════╪═══════════╡
    #   # │ a   ┆ [1, 1]    ┆ [5, 3]    │
    #   # │ b   ┆ [2, 3]    ┆ [4, 2]    │
    #   # │ c   ┆ [3]       ┆ [1]       │
    #   # └─────┴───────────┴───────────┘
    #
    # @example Compute the sum of a column for each group.
    #   ldf.group_by("a").agg(
    #     Polars.col("b").sum
    #   ).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 2   │
    #   # │ b   ┆ 5   │
    #   # │ c   ┆ 3   │
    #   # └─────┴─────┘
    #
    # @example Compute multiple aggregates at once by passing a list of expressions.
    #   ldf.group_by("a").agg(
    #     [Polars.sum("b"), Polars.mean("c")]
    #   ).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ f64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ c   ┆ 3   ┆ 1.0 │
    #   # │ a   ┆ 2   ┆ 4.0 │
    #   # │ b   ┆ 5   ┆ 3.0 │
    #   # └─────┴─────┴─────┘
    #
    # @example Or use positional arguments to compute multiple aggregations in the same way.
    #   ldf.group_by("a").agg(
    #     Polars.sum("b").name.suffix("_sum"),
    #     (Polars.col("c") ** 2).mean.name.suffix("_mean_squared")
    #   ).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬───────┬────────────────┐
    #   # │ a   ┆ b_sum ┆ c_mean_squared │
    #   # │ --- ┆ ---   ┆ ---            │
    #   # │ str ┆ i64   ┆ f64            │
    #   # ╞═════╪═══════╪════════════════╡
    #   # │ a   ┆ 2     ┆ 17.0           │
    #   # │ c   ┆ 3     ┆ 1.0            │
    #   # │ b   ┆ 5     ┆ 10.0           │
    #   # └─────┴───────┴────────────────┘
    #
    # @example Use keyword arguments to easily name your expression inputs.
    #   ldf.group_by("a").agg(
    #     b_sum: Polars.sum("b"),
    #     c_mean_squared: (Polars.col("c") ** 2).mean
    #   ).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬───────┬────────────────┐
    #   # │ a   ┆ b_sum ┆ c_mean_squared │
    #   # │ --- ┆ ---   ┆ ---            │
    #   # │ str ┆ i64   ┆ f64            │
    #   # ╞═════╪═══════╪════════════════╡
    #   # │ a   ┆ 2     ┆ 17.0           │
    #   # │ c   ┆ 3     ┆ 1.0            │
    #   # │ b   ┆ 5     ┆ 10.0           │
    #   # └─────┴───────┴────────────────┘
    def agg(*aggs, **named_aggs)
      rbexprs = Utils.parse_into_list_of_expressions(*aggs, **named_aggs)
      Utils.wrap_ldf(@lgb.agg(rbexprs))
    end

    # Get the first `n` rows of each group.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["c", "c", "a", "c", "a", "b"],
    #       "nrs" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   df.group_by("letters").head(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # │ c       ┆ 1   │
    #   # │ c       ┆ 2   │
    #   # └─────────┴─────┘
    def head(n = 5)
      Utils.wrap_ldf(@lgb.head(n))
    end

    # Get the last `n` rows of each group.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["c", "c", "a", "c", "a", "b"],
    #       "nrs" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   df.group_by("letters").tail(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # │ c       ┆ 2   │
    #   # │ c       ┆ 4   │
    #   # └─────────┴─────┘
    def tail(n = 5)
      Utils.wrap_ldf(@lgb.tail(n))
    end

    # Aggregate the groups into Series.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => ["one", "two", "one", "two"],
    #       "b" => [1, 2, 3, 4]
    #     }
    #   ).lazy
    #   ldf.group_by("a", maintain_order: true).all.collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ b         │
    #   # │ --- ┆ ---       │
    #   # │ str ┆ list[i64] │
    #   # ╞═════╪═══════════╡
    #   # │ one ┆ [1, 3]    │
    #   # │ two ┆ [2, 4]    │
    #   # └─────┴───────────┘
    def all
      agg(F.all)
    end

    # Return the number of rows in each group.
    #
    # @param name [String]
    #   Assign a name to the resulting column; if unset, defaults to "len".
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new({"a" => ["Apple", "Apple", "Orange"], "b" => [1, nil, 2]})
    #   lf.group_by("a").len.collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────┬─────┐
    #   # │ a      ┆ len │
    #   # │ ---    ┆ --- │
    #   # │ str    ┆ u32 │
    #   # ╞════════╪═════╡
    #   # │ Apple  ┆ 2   │
    #   # │ Orange ┆ 1   │
    #   # └────────┴─────┘
    #
    # @example
    #   lf.group_by("a").len(name: "n").collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────┬─────┐
    #   # │ a      ┆ n   │
    #   # │ ---    ┆ --- │
    #   # │ str    ┆ u32 │
    #   # ╞════════╪═════╡
    #   # │ Apple  ┆ 2   │
    #   # │ Orange ┆ 1   │
    #   # └────────┴─────┘
    def len(name: nil)
      len_expr = F.len
      if !name.nil?
        len_expr = len_expr.alias(name)
      end
      agg(len_expr)
    end
  end
end

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

    # Aggregate the first values in the group.
    #
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default `false`).
    #   If set to `true`, the first non-null value for each aggregation is returned,
    #   otherwise `nil` is returned if no non-null value exists.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).first.collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬───────┐
    #   # │ d      ┆ a   ┆ b    ┆ c     │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---   │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞════════╪═════╪══════╪═══════╡
    #   # │ Apple  ┆ 1   ┆ 0.5  ┆ true  │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true  │
    #   # │ Banana ┆ 4   ┆ 13.0 ┆ false │
    #   # └────────┴─────┴──────┴───────┘
    def first(ignore_nulls: false)
      agg(F.all.first(ignore_nulls: ignore_nulls))
    end

    # Aggregate the last values in the group.
    #
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default `false`).
    #   If set to `true`, the last non-null value for each aggregation is returned,
    #   otherwise `nil` is returned if no non-null value exists.
    #
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 14, 13],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).last.collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬───────┐
    #   # │ d      ┆ a   ┆ b    ┆ c     │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---   │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞════════╪═════╪══════╪═══════╡
    #   # │ Apple  ┆ 3   ┆ 10.0 ┆ false │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true  │
    #   # │ Banana ┆ 5   ┆ 13.0 ┆ true  │
    #   # └────────┴─────┴──────┴───────┘
    def last(ignore_nulls: false)
      agg(F.all.last(ignore_nulls: ignore_nulls))
    end

    # Reduce the groups to the maximal value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).max.collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬──────┐
    #   # │ d      ┆ a   ┆ b    ┆ c    │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---  │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool │
    #   # ╞════════╪═════╪══════╪══════╡
    #   # │ Apple  ┆ 3   ┆ 10.0 ┆ true │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true │
    #   # │ Banana ┆ 5   ┆ 14.0 ┆ true │
    #   # └────────┴─────┴──────┴──────┘
    def max
      agg(F.all.max)
    end

    # Reduce the groups to the mean values.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).mean.collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────────┬──────────┐
    #   # │ d      ┆ a   ┆ b        ┆ c        │
    #   # │ ---    ┆ --- ┆ ---      ┆ ---      │
    #   # │ str    ┆ f64 ┆ f64      ┆ f64      │
    #   # ╞════════╪═════╪══════════╪══════════╡
    #   # │ Apple  ┆ 2.0 ┆ 4.833333 ┆ 0.666667 │
    #   # │ Orange ┆ 2.0 ┆ 0.5      ┆ 1.0      │
    #   # │ Banana ┆ 4.5 ┆ 13.5     ┆ 0.5      │
    #   # └────────┴─────┴──────────┴──────────┘
    def mean
      agg(F.all.mean)
    end

    # Return the median per group.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "d" => ["Apple", "Banana", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).median.collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────┬─────┬──────┐
    #   # │ d      ┆ a   ┆ b    │
    #   # │ ---    ┆ --- ┆ ---  │
    #   # │ str    ┆ f64 ┆ f64  │
    #   # ╞════════╪═════╪══════╡
    #   # │ Apple  ┆ 2.0 ┆ 4.0  │
    #   # │ Banana ┆ 4.0 ┆ 13.0 │
    #   # └────────┴─────┴──────┘
    def median
      agg(F.all.median)
    end

    # Reduce the groups to the minimal value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).min.collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬───────┐
    #   # │ d      ┆ a   ┆ b    ┆ c     │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---   │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞════════╪═════╪══════╪═══════╡
    #   # │ Apple  ┆ 1   ┆ 0.5  ┆ false │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true  │
    #   # │ Banana ┆ 4   ┆ 13.0 ┆ false │
    #   # └────────┴─────┴──────┴───────┘
    def min
      agg(F.all.min)
    end

    # Count the unique values per group.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 1, 3, 4, 5],
    #       "b" => [0.5, 0.5, 0.5, 10, 13, 14],
    #       "d" => ["Apple", "Banana", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).n_unique.collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────┬─────┬─────┐
    #   # │ d      ┆ a   ┆ b   │
    #   # │ ---    ┆ --- ┆ --- │
    #   # │ str    ┆ u32 ┆ u32 │
    #   # ╞════════╪═════╪═════╡
    #   # │ Apple  ┆ 2   ┆ 2   │
    #   # │ Banana ┆ 3   ┆ 3   │
    #   # └────────┴─────┴─────┘
    def n_unique
      agg(F.all.n_unique)
    end

    # Compute the quantile per group.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ['nearest', 'higher', 'lower', 'midpoint', 'linear', 'equiprobable']
    #   Interpolation method.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).quantile(1).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────┬──────┐
    #   # │ d      ┆ a   ┆ b    │
    #   # │ ---    ┆ --- ┆ ---  │
    #   # │ str    ┆ f64 ┆ f64  │
    #   # ╞════════╪═════╪══════╡
    #   # │ Apple  ┆ 3.0 ┆ 10.0 │
    #   # │ Orange ┆ 2.0 ┆ 0.5  │
    #   # │ Banana ┆ 5.0 ┆ 14.0 │
    #   # └────────┴─────┴──────┘
    def quantile(quantile, interpolation: "nearest")
      agg(F.all.quantile(quantile, interpolation: interpolation))
    end

    # Reduce the groups to the sum.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   ).lazy
    #   ldf.group_by("d", maintain_order: true).sum.collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬─────┐
    #   # │ d      ┆ a   ┆ b    ┆ c   │
    #   # │ ---    ┆ --- ┆ ---  ┆ --- │
    #   # │ str    ┆ i64 ┆ f64  ┆ u32 │
    #   # ╞════════╪═════╪══════╪═════╡
    #   # │ Apple  ┆ 6   ┆ 14.5 ┆ 2   │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ 1   │
    #   # │ Banana ┆ 9   ┆ 27.0 ┆ 1   │
    #   # └────────┴─────┴──────┴─────┘
    def sum
      agg(F.all.sum)
    end
  end
end

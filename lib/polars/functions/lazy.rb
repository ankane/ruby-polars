module Polars
  module Functions
    # Alias for an element in evaluated in an `eval` expression.
    #
    # @return [Expr]
    #
    # @example A horizontal rank computation by taking the elements of a list
    #   df = Polars::DataFrame.new({"a" => [1, 8, 3], "b" => [4, 5, 2]})
    #   df.with_column(
    #     Polars.concat_list(["a", "b"]).list.eval(Polars.element.rank).alias("rank")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬────────────┐
    #   # │ a   ┆ b   ┆ rank       │
    #   # │ --- ┆ --- ┆ ---        │
    #   # │ i64 ┆ i64 ┆ list[f64]  │
    #   # ╞═════╪═════╪════════════╡
    #   # │ 1   ┆ 4   ┆ [1.0, 2.0] │
    #   # │ 8   ┆ 5   ┆ [2.0, 1.0] │
    #   # │ 3   ┆ 2   ┆ [2.0, 1.0] │
    #   # └─────┴─────┴────────────┘
    def element
      col("")
    end

    # Return the number of non-null values in the column.
    #
    # This function is syntactic sugar for `col(columns).count`.
    #
    # Calling this function without any arguments returns the number of rows in the
    # context. **This way of using the function is deprecated.** Please use `len`
    # instead.
    #
    # @param columns [Array]
    #   One or more column names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil],
    #       "b" => [3, nil, nil],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.count("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    #
    # @example Return the number of non-null values in multiple columns.
    #   df.select(Polars.count("b", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ b   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 3   │
    #   # └─────┴─────┘
    def count(*columns)
      if columns.empty?
        warn "`Polars.count` is deprecated. Use `Polars.length` instead."
        return Utils.wrap_expr(Plr.len._alias("count"))
      end

      col(*columns).count
    end

    # Return the cumulative count of the non-null values in the column.
    #
    # This function is syntactic sugar for `col(columns).cum_count`.
    #
    # If no arguments are passed, returns the cumulative count of a context.
    # Rows containing null values count towards the result.
    #
    # @param columns [Array]
    #   Name(s) of the columns to use.
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, nil], "b" => [3, nil, nil]})
    #   df.select(Polars.cum_count("a"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 2   │
    #   # └─────┘
    def cum_count(*columns, reverse: false)
      col(*columns).cum_count(reverse: reverse)
    end

    # Aggregate all column values into a list.
    #
    # This function is syntactic sugar for `col(name).implode`.
    #
    # @param columns [Array]
    #   One or more column names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => [9, 8, 7],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.implode("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2, 3] │
    #   # └───────────┘
    #
    # @example
    #   df.select(Polars.implode("b", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌───────────┬───────────────────────┐
    #   # │ b         ┆ c                     │
    #   # │ ---       ┆ ---                   │
    #   # │ list[i64] ┆ list[str]             │
    #   # ╞═══════════╪═══════════════════════╡
    #   # │ [9, 8, 7] ┆ ["foo", "bar", "foo"] │
    #   # └───────────┴───────────────────────┘
    def implode(*columns)
      col(*columns).implode
    end

    # Get the standard deviation.
    #
    # This function is syntactic sugar for `col(column).std(ddof: ddof)`.
    #
    # @param column [Object]
    #   Column name.
    # @param ddof [Integer]
    #   “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #   By default ddof is 1.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.std("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 3.605551 │
    #   # └──────────┘
    #
    # @example
    #   df["a"].std
    #   # => 3.605551275463989
    def std(column, ddof: 1)
      col(column).std(ddof: ddof)
    end

    # Get the variance.
    #
    # This function is syntactic sugar for `col(column).var(ddof: ddof)`.
    #
    # @param column [Object]
    #   Column name.
    # @param ddof [Integer]
    #   “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #   By default ddof is 1.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.var("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────┐
    #   # │ a    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ 13.0 │
    #   # └──────┘
    #
    # @example
    #   df["a"].var
    #   # => 13.0
    def var(column, ddof: 1)
      col(column).var(ddof: ddof)
    end


    # Get the mean value.
    #
    # This function is syntactic sugar for `col(columns).mean`.
    #
    # @param columns [Array]
    #   One or more column names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.mean("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 4.0 │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.mean("a", "b"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ b        │
    #   # │ --- ┆ ---      │
    #   # │ f64 ┆ f64      │
    #   # ╞═════╪══════════╡
    #   # │ 4.0 ┆ 3.666667 │
    #   # └─────┴──────────┘
    def mean(*columns)
      col(*columns).mean
    end
    alias_method :avg, :mean

    # Get the median value.
    #
    # This function is syntactic sugar for `pl.col(columns).median`.
    #
    # @param columns [Array]
    #   One or more column names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.median("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 3.0 │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.median("a", "b"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 3.0 ┆ 4.0 │
    #   # └─────┴─────┘
    def median(*columns)
      col(*columns).median
    end

    # Count unique values.
    #
    # This function is syntactic sugar for `col(columns).n_unique`.
    #
    # @param columns [Array]
    #   One or more column names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 1],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.n_unique("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.n_unique("b", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ b   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 2   │
    #   # └─────┴─────┘
    def n_unique(*columns)
      col(*columns).n_unique
    end

    # Approximate count of unique values.
    #
    # This function is syntactic sugar for `col(columns).approx_n_unique`, and
    # uses the HyperLogLog++ algorithm for cardinality estimation.
    #
    # @param columns [Array]
    #   One or more column names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 1],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.approx_n_unique("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.approx_n_unique("b", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ b   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 2   │
    #   # └─────┴─────┘
    def approx_n_unique(*columns)
      col(*columns).approx_n_unique
    end

    # Get the first value.
    #
    # @param columns [Array]
    #   One or more column names. If not provided (default), returns an expression
    #   to take the first column of the context instead.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "baz"]
    #     }
    #   )
    #   df.select(Polars.first)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 8   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.first("b"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ b   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 4   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.first("a", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ str │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ foo │
    #   # └─────┴─────┘
    def first(*columns)
      if columns.empty?
        return Utils.wrap_expr(Plr.first)
      end

      col(*columns).first
    end

    # Get the last value.
    #
    # @param columns [Array]
    #   One or more column names. If set to `nil` (default), returns an expression
    #   to take the last column of the context instead.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "baz"]
    #     }
    #   )
    #   df.select(Polars.last)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ c   │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ foo │
    #   # │ bar │
    #   # │ baz │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.last("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 3   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.last("b", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ b   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ str │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ baz │
    #   # └─────┴─────┘
    def last(*columns)
      if columns.empty?
        return Utils.wrap_expr(Plr.last)
      end

      col(*columns).last
    end

    # Get the first `n` rows.
    #
    # This function is syntactic sugar for `col(column).head(n)`.
    #
    # @param column [Object]
    #   Column name.
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.head("a"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 8   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.head("a", 2))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 8   │
    #   # └─────┘
    def head(column, n = 10)
      col(column).head(n)
    end

    # Get the last `n` rows.
    #
    # This function is syntactic sugar for `col(column).tail(n)`.
    #
    # @param column [Object]
    #   Column name.
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.tail("a"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 8   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.tail("a", 2))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 8   │
    #   # │ 3   │
    #   # └─────┘
    def tail(column, n = 10)
      col(column).tail(n)
    end

    # Compute the Pearson's or Spearman rank correlation correlation between two columns.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   "Delta Degrees of Freedom": the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #   By default ddof is 1.
    # @param method ["pearson", "spearman"]
    #   Correlation method.
    # @param propagate_nans [Boolean]
    #   If `true` any `NaN` encountered will lead to `NaN` in the output.
    #   Defaults to `False` where `NaN` are regarded as larger than any finite number
    #   and thus lead to the highest rank.
    #
    # @return [Expr]
    #
    # @example Pearson's correlation:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.corr("a", "b"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.544705 │
    #   # └──────────┘
    #
    # @example Spearman rank correlation:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.corr("a", "b", method: "spearman"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.5 │
    #   # └─────┘
    def corr(
      a,
      b,
      method: "pearson",
      ddof: 1,
      propagate_nans: false
    )
      a = Utils.parse_as_expression(a)
      b = Utils.parse_as_expression(b)

      if method == "pearson"
        Utils.wrap_expr(Plr.pearson_corr(a, b, ddof))
      elsif method == "spearman"
        Utils.wrap_expr(Plr.spearman_rank_corr(a, b, ddof, propagate_nans))
      else
        msg = "method must be one of {{'pearson', 'spearman'}}, got #{method}"
        raise ArgumentError, msg
      end
    end

    # Compute the covariance between two columns/ expressions.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   "Delta Degrees of Freedom": the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #   By default ddof is 1.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.cov("a", "b"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 3.0 │
    #   # └─────┘
    def cov(a, b, ddof: 1)
      a = Utils.parse_as_expression(a)
      b = Utils.parse_as_expression(b)
      Utils.wrap_expr(Plr.cov(a, b, ddof))
    end

    # def map
    # end

    # def apply
    # end

    # Accumulate over multiple columns horizontally/row wise with a left fold.
    #
    # @return [Expr]
    def fold(acc, f, exprs)
      acc = Utils.expr_to_lit_or_expr(acc, str_to_lit: true)
      if exprs.is_a?(Expr)
        exprs = [exprs]
      end

      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.fold(acc._rbexpr, f, exprs))
    end

    # def reduce
    # end

    # Cumulatively accumulate over multiple columns horizontally/row wise with a left fold.
    #
    # Every cumulative result is added as a separate field in a Struct column.
    #
    # @param acc [Object]
    #   Accumulator Expression. This is the value that will be initialized when the fold
    #   starts. For a sum this could for instance be lit(0).
    # @param f [Object]
    #   Function to apply over the accumulator and the value.
    #   Fn(acc, value) -> new_value
    # @param exprs [Object]
    #   Expressions to aggregate over. May also be a wildcard expression.
    # @param include_init [Boolean]
    #   Include the initial accumulator state as struct field.
    #
    # @return [Object]
    #
    # @note
    #   If you simply want the first encountered expression as accumulator,
    #   consider using `cumreduce`.
    def cum_fold(acc, f, exprs, include_init: false)
      acc = Utils.expr_to_lit_or_expr(acc, str_to_lit: true)
      if exprs.is_a?(Expr)
        exprs = [exprs]
      end

      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.cum_fold(acc._rbexpr, f, exprs, include_init))
    end
    alias_method :cumfold, :cum_fold

    # def cum_reduce
    # end

    # Compute two argument arctan in radians.
    #
    # Returns the angle (in radians) in the plane between the
    # positive x-axis and the ray from the origin to (x,y).
    #
    # @param y [Object]
    #   Column name or Expression.
    # @param x [Object]
    #   Column name or Expression.
    #
    # @return [Expr]
    #
    # @example
    #   twoRootTwo = Math.sqrt(2) / 2
    #   df = Polars::DataFrame.new(
    #     {
    #       "y" => [twoRootTwo, -twoRootTwo, twoRootTwo, -twoRootTwo],
    #       "x" => [twoRootTwo, twoRootTwo, -twoRootTwo, -twoRootTwo]
    #     }
    #   )
    #   df.select(
    #     Polars.arctan2d("y", "x").alias("atan2d"), Polars.arctan2("y", "x").alias("atan2")
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────┬───────────┐
    #   # │ atan2d ┆ atan2     │
    #   # │ ---    ┆ ---       │
    #   # │ f64    ┆ f64       │
    #   # ╞════════╪═══════════╡
    #   # │ 45.0   ┆ 0.785398  │
    #   # │ -45.0  ┆ -0.785398 │
    #   # │ 135.0  ┆ 2.356194  │
    #   # │ -135.0 ┆ -2.356194 │
    #   # └────────┴───────────┘
    def arctan2(y, x)
      if Utils.strlike?(y)
        y = col(y)
      end
      if Utils.strlike?(x)
        x = col(x)
      end
      Utils.wrap_expr(Plr.arctan2(y._rbexpr, x._rbexpr))
    end

    # Compute two argument arctan in degrees.
    #
    # Returns the angle (in degrees) in the plane between the positive x-axis
    # and the ray from the origin to (x,y).
    #
    # @param y [Object]
    #   Column name or Expression.
    # @param x [Object]
    #   Column name or Expression.
    #
    # @return [Expr]
    #
    # @example
    #   twoRootTwo = Math.sqrt(2) / 2
    #   df = Polars::DataFrame.new(
    #     {
    #       "y" => [twoRootTwo, -twoRootTwo, twoRootTwo, -twoRootTwo],
    #       "x" => [twoRootTwo, twoRootTwo, -twoRootTwo, -twoRootTwo]
    #     }
    #   )
    #   df.select(
    #     Polars.arctan2d("y", "x").alias("atan2d"), Polars.arctan2("y", "x").alias("atan2")
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────┬───────────┐
    #   # │ atan2d ┆ atan2     │
    #   # │ ---    ┆ ---       │
    #   # │ f64    ┆ f64       │
    #   # ╞════════╪═══════════╡
    #   # │ 45.0   ┆ 0.785398  │
    #   # │ -45.0  ┆ -0.785398 │
    #   # │ 135.0  ┆ 2.356194  │
    #   # │ -135.0 ┆ -2.356194 │
    #   # └────────┴───────────┘
    def arctan2d(y, x)
      if Utils.strlike?(y)
        y = col(y)
      end
      if Utils.strlike?(x)
        x = col(x)
      end
      Utils.wrap_expr(Plr.arctan2d(y._rbexpr, x._rbexpr))
    end

    # Exclude certain columns from a wildcard/regex selection.
    #
    # @param columns [Object]
    #   Column(s) to exclude from selection
    #   This can be:
    #
    #   - a column name, or multiple column names
    #   - a regular expression starting with `^` and ending with `$`
    #   - a dtype or multiple dtypes
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "aa" => [1, 2, 3],
    #       "ba" => ["a", "b", nil],
    #       "cc" => [nil, 2.5, 1.5]
    #     }
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬──────┬──────┐
    #   # │ aa  ┆ ba   ┆ cc   │
    #   # │ --- ┆ ---  ┆ ---  │
    #   # │ i64 ┆ str  ┆ f64  │
    #   # ╞═════╪══════╪══════╡
    #   # │ 1   ┆ a    ┆ null │
    #   # │ 2   ┆ b    ┆ 2.5  │
    #   # │ 3   ┆ null ┆ 1.5  │
    #   # └─────┴──────┴──────┘
    #
    # @example Exclude by column name(s):
    #   df.select(Polars.exclude("ba"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────┐
    #   # │ aa  ┆ cc   │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ null │
    #   # │ 2   ┆ 2.5  │
    #   # │ 3   ┆ 1.5  │
    #   # └─────┴──────┘
    #
    # @example Exclude by regex, e.g. removing all columns whose names end with the letter "a":
    #   df.select(Polars.exclude("^.*a$"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ cc   │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # │ 2.5  │
    #   # │ 1.5  │
    #   # └──────┘
    def exclude(columns)
      col("*").exclude(columns)
    end

    # Syntactic sugar for `Polars.col("foo").agg_groups`.
    #
    # @return [Object]
    def groups(column)
      col(column).agg_groups
    end

    # Syntactic sugar for `Polars.col("foo").quantile(...)`.
    #
    # @param column [String]
    #   Column name.
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [Expr]
    def quantile(column, quantile, interpolation: "nearest")
      col(column).quantile(quantile, interpolation: interpolation)
    end

    # Find the indexes that would sort the columns.
    #
    # Argsort by multiple columns. The first column will be used for the ordering.
    # If there are duplicates in the first column, the second column will be used to
    # determine the ordering and so on.
    #
    # @param exprs [Object]
    #   Columns use to determine the ordering.
    # @param reverse [Boolean]
    #   Default is ascending.
    #
    # @return [Expr]
    def arg_sort_by(exprs, reverse: false)
      if !exprs.is_a?(::Array)
        exprs = [exprs]
      end
      if reverse == true || reverse == false
        reverse = [reverse] * exprs.length
      end
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.arg_sort_by(exprs, reverse))
    end
    alias_method :argsort_by, :arg_sort_by

    # Collect multiple LazyFrames at the same time.
    #
    # This runs all the computation graphs in parallel on Polars threadpool.
    #
    # @param lazy_frames [Boolean]
    #   A list of LazyFrames to collect.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param string_cache [Boolean]
    #   This argument is deprecated and will be ignored
    # @param no_optimization [Boolean]
    #   Turn off optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param common_subplan_elimination [Boolean]
    #   Will try to cache branching subplans that occur on self-joins or unions.
    # @param allow_streaming [Boolean]
    #   Run parts of the query in a streaming fashion (this is in an alpha state)
    #
    # @return [Array]
    def collect_all(
      lazy_frames,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      string_cache: false,
      no_optimization: false,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
      end

      prepared = []

      lazy_frames.each do |lf|
        ldf = lf._ldf.optimization_toggle(
          type_coercion,
          predicate_pushdown,
          projection_pushdown,
          simplify_expression,
          slice_pushdown,
          common_subplan_elimination,
          allow_streaming,
          false
        )
        prepared << ldf
      end

      out = Plr.collect_all(prepared)

      # wrap the rbdataframes into dataframe
      result = out.map { |rbdf| Utils.wrap_df(rbdf) }

      result
    end

    # Run polars expressions without a context.
    #
    # This is syntactic sugar for running `df.select` on an empty DataFrame.
    #
    # @param exprs [Array]
    #   Column(s) to select, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names,
    #   other non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to select, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    #
    # @example
    #   foo = Polars::Series.new("foo", [1, 2, 3])
    #   bar = Polars::Series.new("bar", [3, 2, 1])
    #   Polars.select(min: Polars.min_horizontal(foo, bar))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ min │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 1   │
    #   # └─────┘
    def select(*exprs, **named_exprs)
      DataFrame.new([]).select(*exprs, **named_exprs)
    end

    # Return indices where `condition` evaluates `true`.
    #
    # @param condition [Expr]
    #   Boolean expression to evaluate
    # @param eager [Boolean]
    #   Whether to apply this function eagerly (as opposed to lazily).
    #
    # @return [Expr, Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4, 5]})
    #   df.select(
    #     [
    #       Polars.arg_where(Polars.col("a") % 2 == 0)
    #     ]
    #   ).to_series
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def arg_where(condition, eager: false)
      if eager
        if !condition.is_a?(Series)
          raise ArgumentError, "expected 'Series' in 'arg_where' if 'eager: true', got #{condition.class.name}"
        end
        condition.to_frame.select(arg_where(Polars.col(condition.name))).to_series
      else
        condition = Utils.expr_to_lit_or_expr(condition, str_to_lit: true)
        Utils.wrap_expr(Plr.arg_where(condition._rbexpr))
      end
    end

    # Folds the columns from left to right, keeping the first non-null value.
    #
    # @param exprs [Array]
    #   Columns to coalesce. Accepts expression input. Strings are parsed as column
    #   names, other non-expression inputs are parsed as literals.
    # @param more_exprs [Hash]
    #   Additional columns to coalesce, specified as positional arguments.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, nil, nil, nil],
    #       "b" => [1, 2, nil, nil],
    #       "c" => [5, nil, 3, nil]
    #     }
    #   )
    #   df.with_columns(Polars.coalesce(["a", "b", "c", 10]).alias("d"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬──────┬─────┐
    #   # │ a    ┆ b    ┆ c    ┆ d   │
    #   # │ ---  ┆ ---  ┆ ---  ┆ --- │
    #   # │ i64  ┆ i64  ┆ i64  ┆ i64 │
    #   # ╞══════╪══════╪══════╪═════╡
    #   # │ 1    ┆ 1    ┆ 5    ┆ 1   │
    #   # │ null ┆ 2    ┆ null ┆ 2   │
    #   # │ null ┆ null ┆ 3    ┆ 3   │
    #   # │ null ┆ null ┆ null ┆ 10  │
    #   # └──────┴──────┴──────┴─────┘
    #
    # @example
    #   df.with_columns(Polars.coalesce(Polars.col(["a", "b", "c"]), 10.0).alias("d"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬──────┬──────┐
    #   # │ a    ┆ b    ┆ c    ┆ d    │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ i64  ┆ f64  │
    #   # ╞══════╪══════╪══════╪══════╡
    #   # │ 1    ┆ 1    ┆ 5    ┆ 1.0  │
    #   # │ null ┆ 2    ┆ null ┆ 2.0  │
    #   # │ null ┆ null ┆ 3    ┆ 3.0  │
    #   # │ null ┆ null ┆ null ┆ 10.0 │
    #   # └──────┴──────┴──────┴──────┘
    def coalesce(exprs, *more_exprs)
      exprs = Utils.parse_as_list_of_expressions(exprs, *more_exprs)
      Utils.wrap_expr(Plr.coalesce(exprs))
    end

    # Utility function that parses an epoch timestamp (or Unix time) to Polars Date(time).
    #
    # Depending on the `unit` provided, this function will return a different dtype:
    # - unit: "d" returns pl.Date
    # - unit: "s" returns pl.Datetime["us"] (pl.Datetime's default)
    # - unit: "ms" returns pl.Datetime["ms"]
    # - unit: "us" returns pl.Datetime["us"]
    # - unit: "ns" returns pl.Datetime["ns"]
    #
    # @param column [Object]
    #     Series or expression to parse integers to pl.Datetime.
    # @param unit [String]
    #     The unit of the timesteps since epoch time.
    # @param eager [Boolean]
    #     If eager evaluation is `true`, a Series is returned instead of an Expr.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new({"timestamp" => [1666683077, 1666683099]}).lazy
    #   df.select(Polars.from_epoch(Polars.col("timestamp"), unit: "s")).collect
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ timestamp           │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2022-10-25 07:31:17 │
    #   # │ 2022-10-25 07:31:39 │
    #   # └─────────────────────┘
    def from_epoch(column, unit: "s", eager: false)
      if Utils.strlike?(column)
        column = col(column)
      elsif !column.is_a?(Series) && !column.is_a?(Expr)
        column = Series.new(column)
      end

      if unit == "d"
        expr = column.cast(Date)
      elsif unit == "s"
        expr = (column.cast(Int64) * 1_000_000).cast(Datetime.new("us"))
      elsif Utils::DTYPE_TEMPORAL_UNITS.include?(unit)
        expr = column.cast(Datetime.new(unit))
      else
        raise ArgumentError, "'unit' must be one of {{'ns', 'us', 'ms', 's', 'd'}}, got '#{unit}'."
      end

      if eager
        if !column.is_a?(Series)
          raise ArgumentError, "expected Series or Array if eager: true, got #{column.class.name}"
        else
          column.to_frame.select(expr).to_series
        end
      else
        expr
      end
    end

    # Parse one or more SQL expressions to polars expression(s).
    #
    # @param sql [Object]
    #   One or more SQL expressions.
    #
    # @return [Expr]
    #
    # @example Parse a single SQL expression:
    #   df = Polars::DataFrame.new({"a" => [2, 1]})
    #   expr = Polars.sql_expr("MAX(a)")
    #   df.select(expr)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    #
    # @example Parse multiple SQL expressions:
    #   df.with_columns(
    #     *Polars.sql_expr(["POWER(a,a) AS a_a", "CAST(a AS TEXT) AS a_txt"])
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ a   ┆ a_a ┆ a_txt │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ i64 ┆ str   │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 2   ┆ 4   ┆ 2     │
    #   # │ 1   ┆ 1   ┆ 1     │
    #   # └─────┴─────┴───────┘
    def sql_expr(sql)
      if sql.is_a?(::String)
        Utils.wrap_expr(Plr.sql_expr(sql))
      else
        sql.map { |q| Utils.wrap_expr(Plr.sql_expr(q)) }
      end
    end
  end
end

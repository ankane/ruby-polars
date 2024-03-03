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

    # Compute the spearman rank correlation between two columns.
    #
    # Missing data will be excluded from the computation.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   Delta degrees of freedom
    # @param propagate_nans [Boolean]
    #   If `True` any `NaN` encountered will lead to `NaN` in the output.
    #   Defaults to `False` where `NaN` are regarded as larger than any finite number
    #   and thus lead to the highest rank.
    #
    # @return [Expr]
    def spearman_rank_corr(a, b, ddof: 1, propagate_nans: false)
      corr(a, b, method: "spearman", ddof: ddof, propagate_nans: propagate_nans)
    end

    # Compute the pearson's correlation between two columns.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   Delta degrees of freedom
    #
    # @return [Expr]
    def pearson_corr(a, b, ddof: 1)
      corr(a, b, method: "pearson", ddof: ddof)
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
  end
end

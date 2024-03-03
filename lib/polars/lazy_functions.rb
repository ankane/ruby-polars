module Polars
  module LazyFunctions
    # Return an expression representing a column in a DataFrame.
    #
    # @return [Expr]
    def col(name, *more_names)
      if more_names.any?
        if Utils.strlike?(name)
          names_str = [name]
          names_str.concat(more_names)
          return Utils.wrap_expr(RbExpr.cols(names_str.map(&:to_s)))
        elsif Utils.is_polars_dtype(name)
          dtypes = [name]
          dtypes.concat(more_names)
          return Utils.wrap_expr(_dtype_cols(dtypes))
        else
          msg = "invalid input for `col`\n\nExpected `str` or `DataType`, got #{name.class.name}."
          raise TypeError, msg
        end
      end

      if Utils.strlike?(name)
        Utils.wrap_expr(RbExpr.col(name.to_s))
      elsif Utils.is_polars_dtype(name)
        Utils.wrap_expr(_dtype_cols([name]))
      elsif name.is_a?(::Array)
        names = Array(name)
        if names.empty?
          return Utils.wrap_expr(RbExpr.cols(names))
        end

        item = names[0]
        if Utils.strlike?(item)
          Utils.wrap_expr(RbExpr.cols(names.map(&:to_s)))
        elsif Utils.is_polars_dtype(item)
          Utils.wrap_expr(_dtype_cols(names))
        else
          msg = "invalid input for `col`\n\nExpected iterable of type `str` or `DataType`, got iterable of type #{item.class.name}."
          raise TypeError, msg
        end
      else
        msg = "invalid input for `col`\n\nExpected `str` or `DataType`, got #{name.class.name}."
        raise TypeError, msg
      end
    end

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
        return Utils.wrap_expr(RbExpr.len._alias("count"))
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

    # Return the number of rows in the context.
    #
    # This is similar to `COUNT(*)` in SQL.
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
    #   df.select(Polars.len)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ len │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 3   │
    #   # └─────┘
    #
    # @example Generate an index column by using `len` in conjunction with `int_range`.
    #   df.select([
    #     Polars.int_range(Polars.len, dtype: Polars::UInt32).alias("index"),
    #     Polars.all
    #   ])
    #   # =>
    #   # shape: (3, 4)
    #   # ┌───────┬──────┬──────┬─────┐
    #   # │ index ┆ a    ┆ b    ┆ c   │
    #   # │ ---   ┆ ---  ┆ ---  ┆ --- │
    #   # │ u32   ┆ i64  ┆ i64  ┆ str │
    #   # ╞═══════╪══════╪══════╪═════╡
    #   # │ 0     ┆ 1    ┆ 3    ┆ foo │
    #   # │ 1     ┆ 2    ┆ null ┆ bar │
    #   # │ 2     ┆ null ┆ null ┆ foo │
    #   # └───────┴──────┴──────┴─────┘
    def len
      Utils.wrap_expr(RbExpr.len)
    end
    alias_method :length, :len

    # Aggregate to list.
    #
    # @return [Expr]
    def to_list(name)
      col(name).list
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
    # @return [Object]
    def std(column, ddof: 1)
      if column.is_a?(Series)
        column.std(ddof: ddof)
      else
        col(column).std(ddof: ddof)
      end
    end

    # Get the variance.
    #
    # @return [Object]
    def var(column, ddof: 1)
      if column.is_a?(Series)
        column.var(ddof: ddof)
      else
        col(column).var(ddof: ddof)
      end
    end

    # Get the maximum value.
    #
    # Syntactic sugar for `col(names).max`.
    #
    # @param names [Array]
    #   Name(s) of the columns to use in the aggregation.
    #
    # @return [Expr]
    #
    # @example Get the maximum value of a column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.max("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 8   │
    #   # └─────┘
    #
    # @example Get the maximum value of multiple columns.
    #   df.select(Polars.max("^a|b$"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 8   ┆ 5   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.max("a", "b"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 8   ┆ 5   │
    #   # └─────┴─────┘
    def max(*names)
      col(*names).max
    end

    # Get the minimum value.
    #
    # Syntactic sugar for `col(names).min`.
    #
    # @param names [Array]
    #   Name(s) of the columns to use in the aggregation.
    #
    # @return [Expr]
    #
    # @example Get the minimum value of a column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, 2],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.min("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # └─────┘
    #
    # @example Get the minimum value of multiple columns.
    #   df.select(Polars.min("^a|b$"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 2   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.min("a", "b"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 2   │
    #   # └─────┴─────┘
    def min(*names)
      col(*names).min
    end

    # Sum all values.
    #
    # Syntactic sugar for `col(name).sum`.
    #
    # @param names [Array]
    #   Name(s) of the columns to use in the aggregation.
    #
    # @return [Expr]
    #
    # @example Sum a column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2],
    #       "b" => [3, 4],
    #       "c" => [5, 6]
    #     }
    #   )
    #   df.select(Polars.sum("a"))
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
    # @example Sum multiple columns.
    #   df.select(Polars.sum("a", "c"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 11  │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.sum("^.*[bc]$"))
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ b   ┆ c   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 7   ┆ 11  │
    #   # └─────┴─────┘
    def sum(*names)
      col(*names).sum
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
        return Utils.wrap_expr(RbExpr.first)
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
        return Utils.wrap_expr(RbExpr.last)
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

    # Return an expression representing a literal value.
    #
    # @return [Expr]
    def lit(value, dtype: nil, allow_object: nil)
      if value.is_a?(::Time) || value.is_a?(::DateTime)
        time_unit = dtype&.time_unit || "ns"
        time_zone = dtype.&time_zone
        e = lit(Utils._datetime_to_pl_timestamp(value, time_unit)).cast(Datetime.new(time_unit))
        if time_zone
          return e.dt.replace_time_zone(time_zone.to_s)
        else
          return e
        end
      elsif value.is_a?(::Date)
        return lit(::Time.utc(value.year, value.month, value.day)).cast(Date)
      elsif value.is_a?(Polars::Series)
        name = value.name
        value = value._s
        e = Utils.wrap_expr(RbExpr.lit(value, allow_object))
        if name == ""
          return e
        end
        return e.alias(name)
      elsif (defined?(Numo::NArray) && value.is_a?(Numo::NArray)) || value.is_a?(::Array)
        return lit(Series.new("", value))
      elsif dtype
        return Utils.wrap_expr(RbExpr.lit(value, allow_object)).cast(dtype)
      end

      Utils.wrap_expr(RbExpr.lit(value, allow_object))
    end

    # Cumulatively sum all values.
    #
    # Syntactic sugar for `col(names).cum_sum`.
    #
    # @param names [Object]
    #   Name(s) of the columns to use in the aggregation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => [4, 5, 6]
    #     }
    #   )
    #   df.select(Polars.cum_sum("a"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 3   │
    #   # │ 6   │
    #   # └─────┘
    def cum_sum(*names)
      col(*names).cum_sum
    end
    alias_method :cumsum, :cum_sum

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
      if Utils.strlike?(a)
        a = col(a)
      end
      if Utils.strlike?(b)
        b = col(b)
      end
      Utils.wrap_expr(RbExpr.spearman_rank_corr(a._rbexpr, b._rbexpr, ddof, propagate_nans))
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
      if Utils.strlike?(a)
        a = col(a)
      end
      if Utils.strlike?(b)
        b = col(b)
      end
      Utils.wrap_expr(RbExpr.pearson_corr(a._rbexpr, b._rbexpr, ddof))
    end

    # Compute the covariance between two columns/ expressions.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    #
    # @return [Expr]
    def cov(a, b)
      if Utils.strlike?(a)
        a = col(a)
      end
      if Utils.strlike?(b)
        b = col(b)
      end
      Utils.wrap_expr(RbExpr.cov(a._rbexpr, b._rbexpr))
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
      Utils.wrap_expr(RbExpr.fold(acc._rbexpr, f, exprs))
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
      Utils.wrap_expr(RbExpr.cum_fold(acc._rbexpr, f, exprs, include_init))
    end
    alias_method :cumfold, :cum_fold

    # def cumreduce
    # end

    # Evaluate a bitwise OR operation.
    #
    # Syntactic sugar for `col(names).any`.
    #
    # @param names [Array]
    #   Name(s) of the columns to use in the aggregation.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [true, false, true],
    #       "b" => [false, false, false]
    #     }
    #   )
    #   df.select(Polars.any("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────┐
    #   # │ a    │
    #   # │ ---  │
    #   # │ bool │
    #   # ╞══════╡
    #   # │ true │
    #   # └──────┘
    def any(*names, ignore_nulls: true)
      col(*names).any(drop_nulls: ignore_nulls)
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

    # Either return an expression representing all columns, or evaluate a bitwise AND operation.
    #
    # If no arguments are passed, this function is syntactic sugar for `col("*")`.
    # Otherwise, this function is syntactic sugar for `col(names).all`.
    #
    # @param names [Array]
    #   Name(s) of the columns to use in the aggregation.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example Selecting all columns.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [true, false, true],
    #       "b" => [false, false, false]
    #     }
    #   )
    #   df.select(Polars.all.sum)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 0   │
    #   # └─────┴─────┘
    #
    # @example Evaluate bitwise AND for a column.
    #   df.select(Polars.all("a"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌───────┐
    #   # │ a     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ false │
    #   # └───────┘
    def all(*names, ignore_nulls: true)
      if names.empty?
        return col("*")
      end

      col(*names).all(drop_nulls: ignore_nulls)
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

    # Create a range expression (or Series).
    #
    # This can be used in a `select`, `with_column`, etc. Be sure that the resulting
    # range size is equal to the length of the DataFrame you are collecting.
    #
    # @param start [Integer, Expr, Series]
    #   Lower bound of range.
    # @param stop [Integer, Expr, Series]
    #   Upper bound of range.
    # @param step [Integer]
    #   Step size of the range.
    # @param eager [Boolean]
    #   If eager evaluation is `True`, a Series is returned instead of an Expr.
    # @param dtype [Symbol]
    #   Apply an explicit integer dtype to the resulting expression (default is `Int64`).
    #
    # @return [Expr, Series]
    #
    # @example
    #   Polars.arange(0, 3, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'arange' [i64]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   # ]
    def int_range(start, stop = nil, step: 1, eager: false, dtype: nil)
      if stop.nil?
        stop = start
        start = 0
      end

      start = Utils.parse_as_expression(start)
      stop = Utils.parse_as_expression(stop)
      dtype ||= Int64
      dtype = dtype.to_s if dtype.is_a?(Symbol)
      result = Utils.wrap_expr(RbExpr.int_range(start, stop, step, dtype)).alias("arange")

      if eager
        return select(result).to_series
      end

      result
    end
    alias_method :arange, :int_range

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
      Utils.wrap_expr(RbExpr.arg_sort_by(exprs, reverse))
    end
    alias_method :argsort_by, :arg_sort_by

    # Create polars `Duration` from distinct time components.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2)],
    #       "add" => [1, 2]
    #     }
    #   )
    #   df.select(
    #     [
    #       (Polars.col("datetime") + Polars.duration(weeks: "add")).alias("add_weeks"),
    #       (Polars.col("datetime") + Polars.duration(days: "add")).alias("add_days"),
    #       (Polars.col("datetime") + Polars.duration(seconds: "add")).alias("add_seconds"),
    #       (Polars.col("datetime") + Polars.duration(milliseconds: "add")).alias(
    #         "add_milliseconds"
    #       ),
    #       (Polars.col("datetime") + Polars.duration(hours: "add")).alias("add_hours")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 5)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬─────────────────────────┬─────────────────────┐
    #   # │ add_weeks           ┆ add_days            ┆ add_seconds         ┆ add_milliseconds        ┆ add_hours           │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---                     ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]            ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪═════════════════════════╪═════════════════════╡
    #   # │ 2022-01-08 00:00:00 ┆ 2022-01-02 00:00:00 ┆ 2022-01-01 00:00:01 ┆ 2022-01-01 00:00:00.001 ┆ 2022-01-01 01:00:00 │
    #   # │ 2022-01-16 00:00:00 ┆ 2022-01-04 00:00:00 ┆ 2022-01-02 00:00:02 ┆ 2022-01-02 00:00:00.002 ┆ 2022-01-02 02:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴─────────────────────────┴─────────────────────┘
    def duration(
      weeks: nil,
      days: nil,
      hours: nil,
      minutes: nil,
      seconds: nil,
      milliseconds: nil,
      microseconds: nil,
      nanoseconds: nil,
      time_unit: "us"
    )
      if !weeks.nil?
        weeks = Utils.expr_to_lit_or_expr(weeks, str_to_lit: false)._rbexpr
      end
      if !days.nil?
        days = Utils.expr_to_lit_or_expr(days, str_to_lit: false)._rbexpr
      end
      if !hours.nil?
        hours = Utils.expr_to_lit_or_expr(hours, str_to_lit: false)._rbexpr
      end
      if !minutes.nil?
        minutes = Utils.expr_to_lit_or_expr(minutes, str_to_lit: false)._rbexpr
      end
      if !seconds.nil?
        seconds = Utils.expr_to_lit_or_expr(seconds, str_to_lit: false)._rbexpr
      end
      if !milliseconds.nil?
        milliseconds = Utils.expr_to_lit_or_expr(milliseconds, str_to_lit: false)._rbexpr
      end
      if !microseconds.nil?
        microseconds = Utils.expr_to_lit_or_expr(microseconds, str_to_lit: false)._rbexpr
      end
      if !nanoseconds.nil?
        nanoseconds = Utils.expr_to_lit_or_expr(nanoseconds, str_to_lit: false)._rbexpr
      end

      Utils.wrap_expr(
        _rb_duration(
          weeks,
          days,
          hours,
          minutes,
          seconds,
          milliseconds,
          microseconds,
          nanoseconds,
          time_unit
        )
      )
    end

    # Horizontally concat Utf8 Series in linear time. Non-Utf8 columns are cast to Utf8.
    #
    # @param exprs [Object]
    #   Columns to concat into a Utf8 Series.
    # @param sep [String]
    #   String value that will be used to separate the values.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["dogs", "cats", nil],
    #       "c" => ["play", "swim", "walk"]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       Polars.concat_str(
    #         [
    #           Polars.col("a") * 2,
    #           Polars.col("b"),
    #           Polars.col("c")
    #         ],
    #         sep: " "
    #       ).alias("full_sentence")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬──────┬───────────────┐
    #   # │ a   ┆ b    ┆ c    ┆ full_sentence │
    #   # │ --- ┆ ---  ┆ ---  ┆ ---           │
    #   # │ i64 ┆ str  ┆ str  ┆ str           │
    #   # ╞═════╪══════╪══════╪═══════════════╡
    #   # │ 1   ┆ dogs ┆ play ┆ 2 dogs play   │
    #   # │ 2   ┆ cats ┆ swim ┆ 4 cats swim   │
    #   # │ 3   ┆ null ┆ walk ┆ null          │
    #   # └─────┴──────┴──────┴───────────────┘
    def concat_str(exprs, sep: "", ignore_nulls: false)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      return Utils.wrap_expr(RbExpr.concat_str(exprs, sep, ignore_nulls))
    end

    # Format expressions as a string.
    #
    # @param fstring [String]
    #   A string that with placeholders.
    #   For example: "hello_{}" or "{}_world
    # @param args [Object]
    #   Expression(s) that fill the placeholders
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a": ["a", "b", "c"],
    #       "b": [1, 2, 3]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.format("foo_{}_bar_{}", Polars.col("a"), "b").alias("fmt")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────┐
    #   # │ fmt         │
    #   # │ ---         │
    #   # │ str         │
    #   # ╞═════════════╡
    #   # │ foo_a_bar_1 │
    #   # │ foo_b_bar_2 │
    #   # │ foo_c_bar_3 │
    #   # └─────────────┘
    def format(fstring, *args)
      if fstring.scan("{}").length != args.length
        raise ArgumentError, "number of placeholders should equal the number of arguments"
      end

      exprs = []

      arguments = args.each
      fstring.split(/(\{\})/).each do |s|
        if s == "{}"
          e = Utils.expr_to_lit_or_expr(arguments.next, str_to_lit: false)
          exprs << e
        elsif s.length > 0
          exprs << lit(s)
        end
      end

      concat_str(exprs, sep: "")
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @return [Expr]
    def concat_list(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.concat_lst(exprs))
    end

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

      out = _collect_all(prepared)

      # wrap the rbdataframes into dataframe
      result = out.map { |rbdf| Utils.wrap_df(rbdf) }

      result
    end

    # Run polars expressions without a context.
    #
    # @return [DataFrame]
    def select(exprs)
      DataFrame.new([]).select(exprs)
    end

    # Collect several columns into a Series of dtype Struct.
    #
    # @param exprs [Object]
    #   Columns/Expressions to collect into a Struct
    # @param eager [Boolean]
    #   Evaluate immediately
    #
    # @return [Object]
    #
    # @example
    #   Polars::DataFrame.new(
    #     {
    #       "int" => [1, 2],
    #       "str" => ["a", "b"],
    #       "bool" => [true, nil],
    #       "list" => [[1, 2], [3]],
    #     }
    #   ).select([Polars.struct(Polars.all).alias("my_struct")])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ my_struct           │
    #   # │ ---                 │
    #   # │ struct[4]           │
    #   # ╞═════════════════════╡
    #   # │ {1,"a",true,[1, 2]} │
    #   # │ {2,"b",null,[3]}    │
    #   # └─────────────────────┘
    #
    # @example Only collect specific columns as a struct:
    #   df = Polars::DataFrame.new(
    #     {"a" => [1, 2, 3, 4], "b" => ["one", "two", "three", "four"], "c" => [9, 8, 7, 6]}
    #   )
    #   df.with_column(Polars.struct(Polars.col(["a", "b"])).alias("a_and_b"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬───────┬─────┬─────────────┐
    #   # │ a   ┆ b     ┆ c   ┆ a_and_b     │
    #   # │ --- ┆ ---   ┆ --- ┆ ---         │
    #   # │ i64 ┆ str   ┆ i64 ┆ struct[2]   │
    #   # ╞═════╪═══════╪═════╪═════════════╡
    #   # │ 1   ┆ one   ┆ 9   ┆ {1,"one"}   │
    #   # │ 2   ┆ two   ┆ 8   ┆ {2,"two"}   │
    #   # │ 3   ┆ three ┆ 7   ┆ {3,"three"} │
    #   # │ 4   ┆ four  ┆ 6   ┆ {4,"four"}  │
    #   # └─────┴───────┴─────┴─────────────┘
    def struct(exprs, eager: false)
      if eager
        Polars.select(struct(exprs, eager: false)).to_series
      end
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(_as_struct(exprs))
    end

    # Repeat a single value n times.
    #
    # @param value [Object]
    #   Value to repeat.
    # @param n [Integer]
    #   Repeat `n` times.
    # @param eager [Boolean]
    #   Run eagerly and collect into a `Series`.
    # @param name [String]
    #   Only used in `eager` mode. As expression, use `alias`.
    #
    # @return [Expr]
    def repeat(value, n, dtype: nil, eager: false, name: nil)
      if !name.nil?
        warn "the `name` argument is deprecated. Use the `alias` method instead."
      end

      if n.is_a?(Integer)
        n = lit(n)
      end

      value = Utils.parse_as_expression(value, str_as_lit: true)
      expr = Utils.wrap_expr(RbExpr.repeat(value, n._rbexpr, dtype))
      if !name.nil?
        expr = expr.alias(name)
      end
      if eager
        return select(expr).to_series
      end
      expr
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
          raise ArgumentError, "expected 'Series' in 'arg_where' if 'eager=True', got #{condition.class.name}"
        end
        condition.to_frame.select(arg_where(Polars.col(condition.name))).to_series
      else
        condition = Utils.expr_to_lit_or_expr(condition, str_to_lit: true)
        Utils.wrap_expr(_arg_where(condition._rbexpr))
      end
    end

    # Folds the expressions from left to right, keeping the first non-null value.
    #
    # @param exprs [Object]
    #   Expressions to coalesce.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     [
    #       [nil, 1.0, 1.0],
    #       [nil, 2.0, 2.0],
    #       [nil, nil, 3.0],
    #       [nil, nil, nil]
    #     ],
    #     columns: [["a", :f64], ["b", :f64], ["c", :f64]]
    #   )
    #   df.with_column(Polars.coalesce(["a", "b", "c", 99.9]).alias("d"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬──────┬──────┐
    #   # │ a    ┆ b    ┆ c    ┆ d    │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---  │
    #   # │ f64  ┆ f64  ┆ f64  ┆ f64  │
    #   # ╞══════╪══════╪══════╪══════╡
    #   # │ null ┆ 1.0  ┆ 1.0  ┆ 1.0  │
    #   # │ null ┆ 2.0  ┆ 2.0  ┆ 2.0  │
    #   # │ null ┆ null ┆ 3.0  ┆ 3.0  │
    #   # │ null ┆ null ┆ null ┆ 99.9 │
    #   # └──────┴──────┴──────┴──────┘
    def coalesce(exprs, *more_exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      if more_exprs.any?
        exprs.concat(Utils.selection_to_rbexpr_list(more_exprs))
      end
      Utils.wrap_expr(_coalesce_exprs(exprs))
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

    # Start a "when, then, otherwise" expression.
    #
    # @return [When]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 3, 4], "bar" => [3, 4, 0]})
    #   df.with_column(Polars.when(Polars.col("foo") > 2).then(Polars.lit(1)).otherwise(Polars.lit(-1)))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────────┐
    #   # │ foo ┆ bar ┆ literal │
    #   # │ --- ┆ --- ┆ ---     │
    #   # │ i64 ┆ i64 ┆ i32     │
    #   # ╞═════╪═════╪═════════╡
    #   # │ 1   ┆ 3   ┆ -1      │
    #   # │ 3   ┆ 4   ┆ 1       │
    #   # │ 4   ┆ 0   ┆ 1       │
    #   # └─────┴─────┴─────────┘
    def when(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      pw = RbExpr.when(expr._rbexpr)
      When.new(pw)
    end

    # Compute the bitwise AND horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [false, false, true, true, false, nil],
    #       "b" => [false, true, true, nil, nil, nil],
    #       "c" => ["u", "v", "w", "x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(all: Polars.all_horizontal("a", "b"))
    #   # =>
    #   # shape: (6, 4)
    #   # ┌───────┬───────┬─────┬───────┐
    #   # │ a     ┆ b     ┆ c   ┆ all   │
    #   # │ ---   ┆ ---   ┆ --- ┆ ---   │
    #   # │ bool  ┆ bool  ┆ str ┆ bool  │
    #   # ╞═══════╪═══════╪═════╪═══════╡
    #   # │ false ┆ false ┆ u   ┆ false │
    #   # │ false ┆ true  ┆ v   ┆ false │
    #   # │ true  ┆ true  ┆ w   ┆ true  │
    #   # │ true  ┆ null  ┆ x   ┆ null  │
    #   # │ false ┆ null  ┆ y   ┆ false │
    #   # │ null  ┆ null  ┆ z   ┆ null  │
    #   # └───────┴───────┴─────┴───────┘
    def all_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      Utils.wrap_expr(_all_horizontal(rbexprs))
    end

    # Compute the bitwise OR horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [false, false, true, true, false, nil],
    #       "b" => [false, true, true, nil, nil, nil],
    #       "c" => ["u", "v", "w", "x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(any: Polars.any_horizontal("a", "b"))
    #   # =>
    #   # shape: (6, 4)
    #   # ┌───────┬───────┬─────┬───────┐
    #   # │ a     ┆ b     ┆ c   ┆ any   │
    #   # │ ---   ┆ ---   ┆ --- ┆ ---   │
    #   # │ bool  ┆ bool  ┆ str ┆ bool  │
    #   # ╞═══════╪═══════╪═════╪═══════╡
    #   # │ false ┆ false ┆ u   ┆ false │
    #   # │ false ┆ true  ┆ v   ┆ true  │
    #   # │ true  ┆ true  ┆ w   ┆ true  │
    #   # │ true  ┆ null  ┆ x   ┆ true  │
    #   # │ false ┆ null  ┆ y   ┆ null  │
    #   # │ null  ┆ null  ┆ z   ┆ null  │
    #   # └───────┴───────┴─────┴───────┘
    def any_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      Utils.wrap_expr(_any_horizontal(rbexprs))
    end

    # Get the maximum value horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(max: Polars.max_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬─────┐
    #   # │ a   ┆ b    ┆ c   ┆ max │
    #   # │ --- ┆ ---  ┆ --- ┆ --- │
    #   # │ i64 ┆ i64  ┆ str ┆ i64 │
    #   # ╞═════╪══════╪═════╪═════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 4   │
    #   # │ 8   ┆ 5    ┆ y   ┆ 8   │
    #   # │ 3   ┆ null ┆ z   ┆ 3   │
    #   # └─────┴──────┴─────┴─────┘
    def max_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      Utils.wrap_expr(_max_horizontal(rbexprs))
    end

    # Get the minimum value horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(min: Polars.min_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬─────┐
    #   # │ a   ┆ b    ┆ c   ┆ min │
    #   # │ --- ┆ ---  ┆ --- ┆ --- │
    #   # │ i64 ┆ i64  ┆ str ┆ i64 │
    #   # ╞═════╪══════╪═════╪═════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 1   │
    #   # │ 8   ┆ 5    ┆ y   ┆ 5   │
    #   # │ 3   ┆ null ┆ z   ┆ 3   │
    #   # └─────┴──────┴─────┴─────┘
    def min_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      Utils.wrap_expr(_min_horizontal(rbexprs))
    end

    # Sum all values horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(sum: Polars.sum_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬─────┐
    #   # │ a   ┆ b    ┆ c   ┆ sum │
    #   # │ --- ┆ ---  ┆ --- ┆ --- │
    #   # │ i64 ┆ i64  ┆ str ┆ i64 │
    #   # ╞═════╪══════╪═════╪═════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 5   │
    #   # │ 8   ┆ 5    ┆ y   ┆ 13  │
    #   # │ 3   ┆ null ┆ z   ┆ 3   │
    #   # └─────┴──────┴─────┴─────┘
    def sum_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      Utils.wrap_expr(_sum_horizontal(rbexprs))
    end

    # Compute the mean of all values horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(mean: Polars.mean_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬──────┐
    #   # │ a   ┆ b    ┆ c   ┆ mean │
    #   # │ --- ┆ ---  ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64  ┆ str ┆ f64  │
    #   # ╞═════╪══════╪═════╪══════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 2.5  │
    #   # │ 8   ┆ 5    ┆ y   ┆ 6.5  │
    #   # │ 3   ┆ null ┆ z   ┆ 3.0  │
    #   # └─────┴──────┴─────┴──────┘
    def mean_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      Utils.wrap_expr(_mean_horizontal(rbexprs))
    end

    # Cumulatively sum all values horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.cum_sum_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬───────────┐
    #   # │ a   ┆ b    ┆ c   ┆ cum_sum   │
    #   # │ --- ┆ ---  ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64  ┆ str ┆ struct[2] │
    #   # ╞═════╪══════╪═════╪═══════════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ {1,5}     │
    #   # │ 8   ┆ 5    ┆ y   ┆ {8,13}    │
    #   # │ 3   ┆ null ┆ z   ┆ {3,null}  │
    #   # └─────┴──────┴─────┴───────────┘
    def cum_sum_horizontal(*exprs)
      rbexprs = Utils.parse_as_list_of_expressions(*exprs)
      exprs_wrapped = rbexprs.map { |e| Utils.wrap_expr(e) }

      # (Expr): use u32 as that will not cast to float as eagerly
      Polars.cum_fold(Polars.lit(0).cast(UInt32), -> (a, b) { a + b }, exprs_wrapped).alias(
        "cum_sum"
      )
    end
    alias_method :cumsum_horizontal, :cum_sum_horizontal
  end
end

module Polars
  module Functions
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

      col(*names).all(ignore_nulls: ignore_nulls)
    end

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
      col(*names).any(ignore_nulls: ignore_nulls)
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
  end
end

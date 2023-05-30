module Polars
  # Namespace for array related expressions.
  class ArrayExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Compute the min values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(2, Polars::Int64)}
    #   )
    #   df.select(Polars.col("a").arr.min)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 3   │
    #   # └─────┘
    def min
      Utils.wrap_expr(_rbexpr.array_min)
    end

    # Compute the max values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(2, Polars::Int64)}
    #   )
    #   df.select(Polars.col("a").arr.max)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # │ 4   │
    #   # └─────┘
    def max
      Utils.wrap_expr(_rbexpr.array_max)
    end

    # Compute the sum values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(2, Polars::Int64)}
    #   )
    #   df.select(Polars.col("a").arr.sum)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 3   │
    #   # │ 7   │
    #   # └─────┘
    def sum
      Utils.wrap_expr(_rbexpr.array_sum)
    end
  end
end

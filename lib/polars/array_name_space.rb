module Polars
  # Series.arr namespace.
  class ArrayNameSpace
    include ExprDispatch

    self._accessor = "arr"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Compute the min values of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, 2], [4, 3]], dtype: Polars::Array.new(2, Polars::Int64)
    #   )
    #   s.arr.min
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def min
      super
    end

    # Compute the max values of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, 2], [4, 3]], dtype: Polars::Array.new(2, Polars::Int64)
    #   )
    #   s.arr.max
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   #         4
    #   # ]
    def max
      super
    end

    # Compute the sum values of the sub-arrays.
    #
    # @return [Series]
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
      super
    end
  end
end

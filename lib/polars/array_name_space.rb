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

    # Get the unique/distinct values in the array.
    #
    # @param maintain_order [Boolean]
    #   Maintain order of data. This requires more work.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 1, 2]]
    #     },
    #     schema_overrides: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.select(Polars.col("a").arr.unique)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2]    │
    #   # └───────────┘
    def unique(maintain_order: false)
      super
    end

    # Convert an Array column into a List column with the same inner data type.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([[1, 2], [3, 4]], dtype: Polars::Array.new(Polars::Int8, 2))
    #   s.arr.to_list
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [list[i8]]
    #   # [
    #   #         [1, 2]
    #   #         [3, 4]
    #   # ]
    def to_list
      super
    end

    # Evaluate whether any boolean value is true for every subarray.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[true, true], [false, true], [false, false], [nil, nil], nil],
    #     dtype: Polars::Array.new(Polars::Boolean, 2)
    #   )
    #   s.arr.any
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   #         false
    #   #         null
    #   # ]
    def any
      super
    end

    # Evaluate whether all boolean values are true for every subarray.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[true, true], [false, true], [false, false], [nil, nil], nil],
    #     dtype: Polars::Array.new(Polars::Boolean, 2)
    #   )
    #   s.arr.all
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         false
    #   #         true
    #   #         null
    #   # ]
    def all
      super
    end

    # Sort the arrays in this column.
    #
    # @param descending [Boolean]
    #   Sort in descending order.
    # @param nulls_last [Boolean]
    #   Place null values last.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [9, 1, 2]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.sort
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [array[i64, 3]]
    #   # [
    #   #         [1, 2, 3]
    #   #         [1, 2, 9]
    #   # ]
    #
    # @example
    #   s.arr.sort(descending: true)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [array[i64, 3]]
    #   # [
    #   #         [3, 2, 1]
    #   #         [9, 2, 1]
    #   # ]
    def sort(descending: false, nulls_last: false)
      super
    end

    # Reverse the arrays in this column.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [9, 1, 2]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.reverse
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [array[i64, 3]]
    #   # [
    #   #         [1, 2, 3]
    #   #         [2, 1, 9]
    #   # ]
    def reverse
      super
    end

    # Retrieve the index of the minimal value in every sub-array.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [9, 1, 2]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.arg_min
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         2
    #   #         1
    #   # ]
    def arg_min
      super
    end

    # Retrieve the index of the maximum value in every sub-array.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[0, 9, 3], [9, 1, 2]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.arg_max
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   #         0
    #   # ]
    def arg_max
      super
    end

    # Get the value by index in the sub-arrays.
    #
    # So index `0` would return the first item of every sublist
    # and index `-1` would return the last item of every sublist
    # if an index is out of bounds, it will return a `nil`.
    #
    # @param index [Integer]
    #   Index to return per sublist
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype: Polars::Array.new(Polars::Int32, 3)
    #   )
    #   s.arr.get(Polars::Series.new([1, -2, 4]))
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i32]
    #   # [
    #   #         2
    #   #         5
    #   #         null
    #   # ]
    def get(index)
      super
    end

    # Get the first value of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype: Polars::Array.new(Polars::Int32, 3)
    #   )
    #   s.arr.first
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i32]
    #   # [
    #   #         1
    #   #         4
    #   #         7
    #   # ]
    def first
      super
    end

    # Get the last value of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype: Polars::Array.new(Polars::Int32, 3)
    #   )
    #   s.arr.last
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i32]
    #   # [
    #   #         3
    #   #         6
    #   #         9
    #   # ]
    def last
      super
    end

    # Join all string items in a sub-array and place a separator between them.
    #
    # This errors if inner type of array `!= String`.
    #
    # @param separator [String]
    #   string to separate the items with
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    #   If set to `False`, null values will be propagated.
    #   If the sub-list contains any null values, the output is `nil`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([["x", "y"], ["a", "b"]], dtype: Polars::Array.new(Polars::String, 2))
    #   s.arr.join("-")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         "x-y"
    #   #         "a-b"
    #   # ]
    def join(separator, ignore_nulls: true)
      super
    end

    # Returns a column with a separate row for every array element.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3], [4, 5, 6]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.explode
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   #         5
    #   #         6
    #   # ]
    def explode
      super
    end

    # Check if sub-arrays contain the given item.
    #
    # @param item [Object]
    #   Item that will be checked for membership
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[3, 2, 1], [1, 2, 3], [4, 5, 6]], dtype: Polars::Array.new(Polars::Int32, 3)
    #   )
    #   s.arr.contains(1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def contains(item)
      super
    end

    # Count how often the value produced by `element` occurs.
    #
    # @param element [Object]
    #   An expression that produces a single value
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3], [2, 2, 2]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.count_matches(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def count_matches(element)
      super
    end
  end
end

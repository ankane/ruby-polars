module Polars
  # Series.arr namespace.
  class ListNameSpace
    include ExprDispatch

    self._accessor = "arr"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Get the length of the arrays as UInt32.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([[1, 2, 3], [5]])
    #   s.arr.lengths
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [u32]
    #   # [
    #   #         3
    #   #         1
    #   # ]
    def lengths
      super
    end

    # Sum all the arrays in the list.
    #
    # @return [Series]
    def sum
      super
    end

    # Compute the max value of the arrays in the list.
    #
    # @return [Series]
    def max
      super
    end

    # Compute the min value of the arrays in the list.
    #
    # @return [Series]
    def min
      super
    end

    # Compute the mean value of the arrays in the list.
    #
    # @return [Series]
    def mean
      super
    end

    # Sort the arrays in the list.
    #
    # @return [Series]
    def sort(reverse: false)
      super
    end

    # Reverse the arrays in the list.
    #
    # @return [Series]
    def reverse
      super
    end

    # Get the unique/distinct values in the list.
    #
    # @return [Series]
    def unique
      super
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @param other [Object]
    #   Columns to concat into a List Series
    #
    # @return [Series]
    def concat(other)
      super
    end

    # Get the value by index in the sublists.
    #
    # So index `0` would return the first item of every sublist
    # and index `-1` would return the last item of every sublist
    # if an index is out of bounds, it will return a `None`.
    #
    # @param index [Integer]
    #   Index to return per sublist
    #
    # @return [Series]
    def get(index)
      super
    end

    # Get the value by index in the sublists.
    #
    # @return [Series]
    def [](item)
      get(item)
    end

    # Join all string items in a sublist and place a separator between them.
    #
    # This errors if inner type of list `!= Utf8`.
    #
    # @param separator [String]
    #   string to separate the items with
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([["foo", "bar"], ["hello", "world"]])
    #   s.arr.join("-")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         "foo-bar"
    #   #         "hello-world"
    #   # ]
    def join(separator)
      super
    end

    # Get the first value of the sublists.
    #
    # @return [Series]
    def first
      super
    end

    # Get the last value of the sublists.
    #
    # @return [Series]
    def last
      super
    end

    # Check if sublists contain the given item.
    #
    # @param item [Object]
    #   Item that will be checked for membership.
    #
    # @return [Series]
    def contains(item)
      super
    end

    # Retrieve the index of the minimal value in every sublist.
    #
    # @return [Series]
    def arg_min
      super
    end

    # Retrieve the index of the maximum value in every sublist.
    #
    # @return [Series]
    def arg_max
      super
    end

    # Calculate the n-th discrete difference of every sublist.
    #
    # @param n [Integer]
    #   Number of slots to shift.
    # @param null_behavior ["ignore", "drop"]
    #   How to handle null values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.diff
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [null, 1, … 1]
    #   #         [null, -8, -1]
    #   # ]
    def diff(n: 1, null_behavior: "ignore")
      super
    end

    # Shift values by the given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.shift
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [null, 1, … 3]
    #   #         [null, 10, 2]
    #   # ]
    def shift(periods = 1)
      super
    end

    # Slice every sublist.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.slice(1, 2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [2, 3]
    #   #         [2, 1]
    #   # ]
    def slice(offset, length = nil)
      super
    end

    # Slice the first `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.head(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [1, 2]
    #   #         [10, 2]
    #   # ]
    def head(n = 5)
      super
    end

    # Slice the last `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.tail(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [3, 4]
    #   #         [2, 1]
    #   # ]
    def tail(n = 5)
      super
    end

    # Convert the series of type `List` to a series of type `Struct`.
    #
    # @param n_field_strategy ["first_non_null", "max_width"]
    #   Strategy to determine the number of fields of the struct.
    # @param name_generator [Object]
    #   A custom function that can be used to generate the field names.
    #   Default field names are `field_0, field_1 .. field_n`
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [[1, 2, 3], [1, 2]]})
    #   df.select([Polars.col("a").arr.to_struct])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────────┐
    #   # │ a          │
    #   # │ ---        │
    #   # │ struct[3]  │
    #   # ╞════════════╡
    #   # │ {1,2,3}    │
    #   # │ {1,2,null} │
    #   # └────────────┘
    def to_struct(n_field_strategy: "first_non_null", name_generator: nil)
      super
    end

    # Run any polars expression against the lists' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.first`, or
    #   `Polars.col`
    # @param parallel [Boolean]
    #   Run all expression parallel. Don't activate this blindly.
    #   Parallelism is worth it if there is enough work to do per thread.
    #
    #   This likely should not be use in the groupby context, because we already
    #   parallel execution per group
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 8, 3], "b" => [4, 5, 2]})
    #   df.with_column(
    #     Polars.concat_list(["a", "b"]).arr.eval(Polars.element.rank).alias("rank")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬────────────┐
    #   # │ a   ┆ b   ┆ rank       │
    #   # │ --- ┆ --- ┆ ---        │
    #   # │ i64 ┆ i64 ┆ list[f32]  │
    #   # ╞═════╪═════╪════════════╡
    #   # │ 1   ┆ 4   ┆ [1.0, 2.0] │
    #   # │ 8   ┆ 5   ┆ [2.0, 1.0] │
    #   # │ 3   ┆ 2   ┆ [2.0, 1.0] │
    #   # └─────┴─────┴────────────┘
    def eval(expr, parallel: false)
      super
    end
  end
end

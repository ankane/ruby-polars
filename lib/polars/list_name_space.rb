module Polars
  # Series.list namespace.
  class ListNameSpace
    include ExprDispatch

    self._accessor = "list"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Evaluate whether all boolean values in a list are true.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[true, true], [false, true], [false, false], [nil], [], nil],
    #     dtype: Polars::List.new(Polars::Boolean)
    #   )
    #   s.list.all
    #   # =>
    #   # shape: (6,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         false
    #   #         true
    #   #         true
    #   #         null
    #   # ]
    def all
      super
    end

    # Evaluate whether any boolean value in a list is true.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[true, true], [false, true], [false, false], [nil], [], nil],
    #     dtype: Polars::List.new(Polars::Boolean)
    #   )
    #   s.list.any
    #   # =>
    #   # shape: (6,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   #         false
    #   #         false
    #   #         null
    #   # ]
    def any
      super
    end

    # Get the length of the arrays as UInt32.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([[1, 2, 3], [5]])
    #   s.list.lengths
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

    # Drop all null values in the list.
    #
    # The original order of the remaining elements is preserved.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[nil, 1, nil, 2], [nil], [3, 4]])
    #   s.list.drop_nulls
    #   # =>
    #   # shape: (3,)
    #   # Series: 'values' [list[i64]]
    #   # [
    #   #         [1, 2]
    #   #         []
    #   #         [3, 4]
    #   # ]
    def drop_nulls
      super
    end

    # Sample from this list.
    #
    # @param n [Integer]
    #   Number of items to return. Cannot be used with `fraction`. Defaults to 1 if
    #   `fraction` is nil.
    # @param fraction [Float]
    #   Fraction of items to return. Cannot be used with `n`.
    # @param with_replacement [Boolean]
    #   Allow values to be sampled more than once.
    # @param shuffle [Boolean]
    #   Shuffle the order of sampled data points.
    # @param seed [Integer]
    #   Seed for the random number generator. If set to nil (default), a
    #   random seed is generated for each sample operation.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[1, 2, 3], [4, 5]])
    #   s.list.sample(n: Polars::Series.new("n", [2, 1]), seed: 1)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [list[i64]]
    #   # [
    #   #         [2, 1]
    #   #         [5]
    #   # ]
    def sample(n: nil, fraction: nil, with_replacement: false, shuffle: false, seed: nil)
      super
    end

    # Sum all the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[1], [2, 3]])
    #   s.list.sum
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [i64]
    #   # [
    #   #         1
    #   #         5
    #   # ]
    def sum
      super
    end

    # Compute the max value of the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[4, 1], [2, 3]])
    #   s.list.max
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [i64]
    #   # [
    #   #         4
    #   #         3
    #   # ]
    def max
      super
    end

    # Compute the min value of the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[4, 1], [2, 3]])
    #   s.list.min
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [i64]
    #   # [
    #   #         1
    #   #         2
    #   # ]
    def min
      super
    end

    # Compute the mean value of the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[3, 1], [3, 3]])
    #   s.list.mean
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [f64]
    #   # [
    #   #         2.0
    #   #         3.0
    #   # ]
    def mean
      super
    end

    # Sort the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [9, 1, 2]])
    #   s.list.sort
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [1, 2, 3]
    #   #         [1, 2, 9]
    #   # ]
    #
    # @example
    #   s.list.sort(reverse: true)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [3, 2, 1]
    #   #         [9, 2, 1]
    #   # ]
    def sort(reverse: false)
      super
    end

    # Reverse the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [9, 1, 2]])
    #   s.list.reverse
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [1, 2, 3]
    #   #         [2, 1, 9]
    #   # ]
    def reverse
      super
    end

    # Get the unique/distinct values in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 1, 2], [2, 3, 3]])
    #   s.list.unique()
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [1, 2]
    #   #         [2, 3]
    #   # ]
    def unique
      super
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @param other [Object]
    #   Columns to concat into a List Series
    #
    # @return [Series]
    #
    # @example
    #   s1 = Polars::Series.new("a", [["a", "b"], ["c"]])
    #   s2 = Polars::Series.new("b", [["c"], ["d", nil]])
    #   s1.list.concat(s2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[str]]
    #   # [
    #   #         ["a", "b", "c"]
    #   #         ["c", "d", null]
    #   # ]
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
    # @param null_on_oob [Boolean]
    #   Behavior if an index is out of bounds:
    #   true -> set as null
    #   false -> raise an error
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [], [1, 2]])
    #   s.list.get(0, null_on_oob: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         3
    #   #         null
    #   #         1
    #   # ]
    def get(index, null_on_oob: false)
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
    #   s.list.join("-")
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
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [], [1, 2]])
    #   s.list.first
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         3
    #   #         null
    #   #         1
    #   # ]
    def first
      super
    end

    # Get the last value of the sublists.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [], [1, 2]])
    #   s.list.last
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         null
    #   #         2
    #   # ]
    def last
      super
    end

    # Check if sublists contain the given item.
    #
    # @param item [Object]
    #   Item that will be checked for membership.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [], [1, 2]])
    #   s.list.contains(1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         true
    #   # ]
    def contains(item)
      super
    end

    # Retrieve the index of the minimal value in every sublist.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [2, 1]])
    #   s.list.arg_min
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         0
    #   #         1
    #   # ]
    def arg_min
      super
    end

    # Retrieve the index of the maximum value in every sublist.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [2, 1]])
    #   s.list.arg_max
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
    #   s.list.diff
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
    #   s.list.shift
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
    #   s.list.slice(1, 2)
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
    #   s.list.head(2)
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
    #   s.list.tail(2)
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
    #   df.select([Polars.col("a").list.to_struct])
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
    #
    # @return [Series]
    #
    # @example
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
    def eval(expr)
      super
    end
  end
end

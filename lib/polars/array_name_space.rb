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
    #     "a", [[1, 2], [4, 3]], dtype: Polars::Array.new(Polars::Int64, 2)
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
    #     "a", [[1, 2], [4, 3]], dtype: Polars::Array.new(Polars::Int64, 2)
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
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
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

    # Compute the std of the values of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [4, 3]], dtype: Polars::Array.new(Polars::Int64, 2))
    #   s.arr.std
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.707107
    #   #         0.707107
    #   # ]
    def std(ddof: 1)
      super
    end

    # Compute the var of the values of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [4, 3]], dtype: Polars::Array.new(Polars::Int64, 2))
    #   s.arr.var
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.5
    #   #         0.5
    #   # ]
    def var(ddof: 1)
      super
    end

    # Compute the median of the values of the sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [4, 3]], dtype: Polars::Array.new(Polars::Int64, 2))
    #   s.arr.median
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.5
    #   #         3.5
    #   # ]
    def median
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

    # Count the number of unique values in every sub-arrays.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [4, 4]], dtype: Polars::Array.new(Polars::Int64, 2))
    #   s.arr.n_unique
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         2
    #   #         1
    #   # ]
    def n_unique
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

    # Return the number of elements in each array.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [4, 3]], dtype: Polars::Array.new(Polars::Int8, 2))
    #   s.arr.len
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         2
    #   #         2
    #   # ]
    def len
      super
    end

    # Slice the sub-arrays.
    #
    # @param offset [Integer]
    #   The starting index of the slice.
    # @param length [Integer]
    #   The length of the slice.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12]],
    #     dtype: Polars::Array.new(Polars::Int64, 6)
    #   )
    #   s.arr.slice(1)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [2, 3, … 6]
    #   #         [8, 9, … 12]
    #   # ]
    #
    # @example
    #   s.arr.slice(1, 3, as_array: true)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [array[i64, 3]]
    #   # [
    #   #         [2, 3, 4]
    #   #         [8, 9, 10]
    #   # ]
    #
    # @example
    #   s.arr.slice(-2)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [5, 6]
    #   #         [11, 12]
    #   # ]
    def slice(
      offset,
      length = nil,
      as_array: false
    )
      super
    end

    # Get the first `n` elements of the sub-arrays.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    # @param as_array [Boolean]
    #   Return result as a fixed-length `Array`, otherwise as a `List`.
    #   If true `n` must be a constant value.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12]],
    #     dtype: Polars::Array.new(Polars::Int64, 6)
    #   )
    #   s.arr.head
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [1, 2, … 5]
    #   #         [7, 8, … 11]
    #   # ]
    #
    # @example
    #   s.arr.head(3, as_array: true)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [array[i64, 3]]
    #   # [
    #   #         [1, 2, 3]
    #   #         [7, 8, 9]
    #   # ]
    def head(n = 5, as_array: false)
      super
    end

    # Slice the last `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    # @param as_array [Boolean]
    #   Return result as a fixed-length `Array`, otherwise as a `List`.
    #   If true `n` must be a constant value.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [[1, 2, 3, 4, 5, 6], [7, 8, 9, 10, 11, 12]],
    #     dtype: Polars::Array.new(Polars::Int64, 6)
    #   )
    #   s.arr.tail
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [2, 3, … 6]
    #   #         [8, 9, … 12]
    #   # ]
    #
    # @example
    #   s.arr.tail(3, as_array: true)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [array[i64, 3]]
    #   # [
    #   #         [4, 5, 6]
    #   #         [10, 11, 12]
    #   # ]
    def tail(n = 5, as_array: false)
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
    # @param null_on_oob [Boolean]
    #   Behavior if an index is out of bounds:
    #   true -> set as null
    #   false -> raise an error
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype: Polars::Array.new(Polars::Int32, 3)
    #   )
    #   s.arr.get(Polars::Series.new([1, -2, 4]), null_on_oob: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i32]
    #   # [
    #   #         2
    #   #         5
    #   #         null
    #   # ]
    def get(index, null_on_oob: false)
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
    #   If set to `false`, null values will be propagated.
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
    # @param nulls_equal [Boolean]
    #   If true, treat null as a distinct value. Null values will not propagate.
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
    def contains(item, nulls_equal: true)
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

    # Convert the series of type `Array` to a series of type `Struct`.
    #
    # @param fields [Object]
    #   If the name and number of the desired fields is known in advance
    #   a list of field names can be given, which will be assigned by index.
    #   Otherwise, to dynamically assign field names, a custom function can be
    #   used; if neither are set, fields will be `field_0, field_1 .. field_n`.
    #
    # @return [Series]
    #
    # @example Convert array to struct with default field name assignment:
    #   s1 = Polars::Series.new("n", [[0, 1, 2], [3, 4, 5]], dtype: Polars::Array.new(Polars::Int8, 3))
    #   s2 = s1.arr.to_struct
    #   # =>
    #   # shape: (2,)
    #   # Series: 'n' [struct[3]]
    #   # [
    #   #         {0,1,2}
    #   #         {3,4,5}
    #   # ]
    #
    # @example
    #   s2.struct.fields
    #   # => ["field_0", "field_1", "field_2"]
    def to_struct(
      fields: nil
    )
      s = Utils.wrap_s(_s)
      s.to_frame.select(F.col(s.name).arr.to_struct(fields: fields)).to_series
    end

    # Shift array values by the given number of indices.
    #
    # @param n [Integer]
    #   Number of indices to shift forward. If a negative value is passed, values
    #   are shifted in the opposite direction instead.
    #
    # @return [Series]
    #
    # @note
    #   This method is similar to the `LAG` operation in SQL when the value for `n`
    #   is positive. With a negative value for `n`, it is similar to `LEAD`.
    #
    # @example By default, array values are shifted forward by one index.
    #   s = Polars::Series.new([[1, 2, 3], [4, 5, 6]], dtype: Polars::Array.new(Polars::Int64, 3))
    #   s.arr.shift
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [array[i64, 3]]
    #   # [
    #   #         [null, 1, 2]
    #   #         [null, 4, 5]
    #   # ]
    #
    # @example Pass a negative value to shift in the opposite direction instead.
    #   s.arr.shift(-2)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [array[i64, 3]]
    #   # [
    #   #         [3, null, null]
    #   #         [6, null, null]
    #   # ]
    def shift(n = 1)
      super
    end

    # Run any polars expression against the arrays' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `pl.element()`
    # @param as_list [Boolean]
    #   Collect the resulting data as a list. This allows for expressions which
    #   output a variable amount of data.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 4], [8, 5], [3, 2]], dtype: Polars::Array.new(Polars::Int64, 2))
    #   s.arr.eval(Polars.element.rank)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [array[f64, 2]]
    #   # [
    #   #         [1.0, 2.0]
    #   #         [2.0, 1.0]
    #   #         [2.0, 1.0]
    #   # ]
    def eval(expr, as_list: false)
      s = Utils.wrap_s(_s)
      s.to_frame.select(F.col(s.name).arr.eval(expr, as_list: as_list)).to_series
    end

    # Run any polars aggregation expression against the arrays' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.element`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     "a", [[1, nil], [42, 13], [nil, nil]], dtype: Polars::Array.new(Polars::Int64, 2)
    #   )
    #   s.arr.agg(Polars.element.null_count)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   #         0
    #   #         2
    #   # ]
    #
    # @example
    #   s.arr.agg(Polars.element.drop_nulls)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [1]
    #   #         [42, 13]
    #   #         []
    #   # ]
    def agg(expr)
      super
    end
  end
end

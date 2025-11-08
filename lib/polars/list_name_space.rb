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
    #   s.list.len
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [u32]
    #   # [
    #   #         3
    #   #         1
    #   # ]
    def len
      super
    end
    alias_method :lengths, :len

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
    #   #         [2, 3]
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

    # Compute the median value of the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[-1, 0, 1], [1, 10]])
    #   s.list.median
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [f64]
    #   # [
    #   #         0.0
    #   #         5.5
    #   # ]
    def median
      super
    end

    # Compute the std value of the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[-1, 0, 1], [1, 10]])
    #   s.list.std
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [f64]
    #   # [
    #   #         1.0
    #   #         6.363961
    #   # ]
    def std(ddof: 1)
      super
    end

    # Compute the var value of the arrays in the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", [[-1, 0, 1], [1, 10]])
    #   s.list.var
    #   # =>
    #   # shape: (2,)
    #   # Series: 'values' [f64]
    #   # [
    #   #         1.0
    #   #         40.5
    #   # ]
    def var(ddof: 1)
      super
    end

    # Sort the arrays in the list.
    #
    # @param reverse [Boolean]
    #   Sort in descending order.
    # @param nulls_last [Boolean]
    #   Place null values last.
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
    def sort(reverse: false, nulls_last: false)
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
    # @param maintain_order [Boolean]
    #   Maintain order of data. This requires more work.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 1, 2], [2, 3, 3]])
    #   s.list.unique
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [1, 2]
    #   #         [2, 3]
    #   # ]
    def unique(maintain_order: false)
      super
    end

    # Count the number of unique values in every sub-lists.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 1, 2], [2, 3, 4]])
    #   s.list.n_unique
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         2
    #   #         3
    #   # ]
    def n_unique
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

    # Take sublists by multiple indices.
    #
    # The indices may be defined in a single column, or by sublists in another
    # column of dtype `List`.
    #
    # @param indices [Object]
    #   Indices to return per sublist
    # @param null_on_oob [Boolean]
    #   Behavior if an index is out of bounds:
    #   True -> set as null
    #   False -> raise an error
    #   Note that defaulting to raising an error is much cheaper
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[3, 2, 1], [], [1, 2]])
    #   s.list.gather([0, 2], null_on_oob: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [3, 1]
    #   #         [null, null]
    #   #         [1, null]
    #   # ]
    def gather(
      indices,
      null_on_oob: false
    )
      super
    end

    # Take every n-th value start from offset in sublists.
    #
    # @param n [Integer]
    #   Gather every n-th element.
    # @param offset [Integer]
    #   Starting index.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3], [], [6, 7, 8, 9]])
    #   s.list.gather_every(2, 1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [2]
    #   #         []
    #   #         [7, 9]
    #   # ]
    def gather_every(n, offset = 0)
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
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    #   If set to `false`, null values will be propagated.
    #   If the sub-list contains any null values, the output is `nil`.
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
    def join(separator, ignore_nulls: true)
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

    # Get the single value of the sublists.
    #
    # This errors if the sublist length is not exactly one.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1], [4], [6]])
    #   s.list.item
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         4
    #   #         6
    #   # ]
    def item
      super
    end

    # Check if sublists contain the given item.
    #
    # @param item [Object]
    #   Item that will be checked for membership.
    # @param nulls_equal [Boolean]
    #   If true, treat null as a distinct value. Null values will not propagate.
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
    def contains(item, nulls_equal: true)
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
    # @param n [Integer]
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
    def shift(n = 1)
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

    # Returns a column with a separate row for every list element.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3], [4, 5, 6]])
    #   s.list.explode
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

    # Count how often the value produced by `element` occurs.
    #
    # @param element [Object]
    #   An expression that produces a single value
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[0], [1], [1, 2, 3, 2], [1, 2, 1], [4, 4]])
    #   s.list.count_matches(1)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         0
    #   #         1
    #   #         1
    #   #         2
    #   #         0
    #   # ]
    def count_matches(element)
      super
    end

    # Convert a List column into an Array column with the same inner data type.
    #
    # @param width [Integer]
    #   Width of the resulting Array column.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([[1, 2], [3, 4]], dtype: Polars::List.new(Polars::Int8))
    #   s.list.to_array(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [array[i8, 2]]
    #   # [
    #   #         [1, 2]
    #   #         [3, 4]
    #   # ]
    def to_array(width)
      super
    end

    # Convert the series of type `List` to a series of type `Struct`.
    #
    # @param n_field_strategy ["first_non_null", "max_width"]
    #   Strategy to determine the number of fields of the struct.
    # @param fields [Object]
    #   If the name and number of the desired fields is known in advance
    #   a list of field names can be given, which will be assigned by index.
    #   Otherwise, to dynamically assign field names, a custom function can be
    #   used; if neither are set, fields will be `field_0, field_1 .. field_n`.
    #
    # @return [Series]
    #
    # @example Convert list to struct with field name assignment by index from a list of names:
    #   s1 = Polars::Series.new("n", [[0, 1, 2], [0, 1]])
    #   s1.list.to_struct(fields: ["one", "two", "three"]).struct.unnest
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ one ┆ two ┆ three │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ i64 ┆ i64   │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 0   ┆ 1   ┆ 2     │
    #   # │ 0   ┆ 1   ┆ null  │
    #   # └─────┴─────┴───────┘
    def to_struct(n_field_strategy: "first_non_null", fields: nil)
      if fields.is_a?(::Array)
        s = Utils.wrap_s(_s)
        return (
          s.to_frame
          .select_seq(F.col(s.name).list.to_struct(fields: fields))
          .to_series
        )
      end

      raise Todo
      # Utils.wrap_s(_s.list_to_struct(n_field_strategy, fields))
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
    #   s = Polars::Series.new("a", [[1, 4], [8, 5], [3, 2]])
    #   s.list.eval(Polars.element.rank)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [list[f64]]
    #   # [
    #   #         [1.0, 2.0]
    #   #         [2.0, 1.0]
    #   #         [2.0, 1.0]
    #   # ]
    def eval(expr)
      s = Utils.wrap_s(_s)
      s.to_frame.select(F.col(s.name).list.eval(expr)).to_series
    end

    # Run any polars aggregation expression against the list' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.element`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, nil], [42, 13], [nil, nil]])
    #   s.list.agg(Polars.element.null_count)
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
    #   s.list.agg(Polars.element.drop_nulls)
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

    # Filter elements in each list by a boolean expression, returning a new Series of lists.
    #
    # @param predicate [Object]
    #   A boolean expression evaluated on each list element.
    #   Use `Polars.element` to refer to the current element.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 4], [8, 5], [3, 2]])
    #   s.list.filter(Polars.element % 2 == 0)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [4]
    #   #         [8]
    #   #         [2]
    #   # ]
    def filter(predicate)
      super
    end

    # Compute the SET UNION between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Series]
    #
    # @example
    #   a = Polars::Series.new([[1, 2, 3], [], [nil, 3], [5, 6, 7]])
    #   b = Polars::Series.new([[2, 3, 4], [3], [3, 4, nil], [6, 8]])
    #   a.list.set_union(b)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [1, 2, … 4]
    #   #         [3]
    #   #         [null, 3, 4]
    #   #         [5, 6, … 8]
    #   # ]
    def set_union(other)
      super
    end

    # Compute the SET DIFFERENCE between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Series]
    #
    # @example
    #   a = Polars::Series.new([[1, 2, 3], [], [nil, 3], [5, 6, 7]])
    #   b = Polars::Series.new([[2, 3, 4], [3], [3, 4, nil], [6, 8]])
    #   a.list.set_difference(b)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [1]
    #   #         []
    #   #         []
    #   #         [5, 7]
    #   # ]
    def set_difference(other)
      super
    end

    # Compute the SET INTERSECTION between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Series]
    #
    # @example
    #   a = Polars::Series.new([[1, 2, 3], [], [nil, 3], [5, 6, 7]])
    #   b = Polars::Series.new([[2, 3, 4], [3], [3, 4, nil], [6, 8]])
    #   a.list.set_intersection(b)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [2, 3]
    #   #         []
    #   #         [null, 3]
    #   #         [6]
    #   # ]
    def set_intersection(other)
      super
    end

    # Compute the SET SYMMETRIC DIFFERENCE between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Series]
    #
    # @example
    #   a = Polars::Series.new([[1, 2, 3], [], [nil, 3], [5, 6, 7]])
    #   b = Polars::Series.new([[2, 3, 4], [3], [3, 4, nil], [6, 8]])
    #   a.list.set_symmetric_difference(b)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [1, 4]
    #   #         [3]
    #   #         [4]
    #   #         [5, 7, 8]
    #   # ]
    def set_symmetric_difference(other)
      super
    end
  end
end

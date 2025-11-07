module Polars
  # Namespace for list related expressions.
  class ListExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Evaluate whether all boolean values in a list are true.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[true, true], [false, true], [false, false], [nil], [], nil]}
    #   )
    #   df.with_columns(all: Polars.col("a").list.all)
    #   # =>
    #   # shape: (6, 2)
    #   # ┌────────────────┬───────┐
    #   # │ a              ┆ all   │
    #   # │ ---            ┆ ---   │
    #   # │ list[bool]     ┆ bool  │
    #   # ╞════════════════╪═══════╡
    #   # │ [true, true]   ┆ true  │
    #   # │ [false, true]  ┆ false │
    #   # │ [false, false] ┆ false │
    #   # │ [null]         ┆ true  │
    #   # │ []             ┆ true  │
    #   # │ null           ┆ null  │
    #   # └────────────────┴───────┘
    def all
      Utils.wrap_expr(_rbexpr.list_all)
    end

    # Evaluate whether any boolean value in a list is true.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[true, true], [false, true], [false, false], [nil], [], nil]}
    #   )
    #   df.with_columns(any: Polars.col("a").list.any)
    #   # =>
    #   # shape: (6, 2)
    #   # ┌────────────────┬───────┐
    #   # │ a              ┆ any   │
    #   # │ ---            ┆ ---   │
    #   # │ list[bool]     ┆ bool  │
    #   # ╞════════════════╪═══════╡
    #   # │ [true, true]   ┆ true  │
    #   # │ [false, true]  ┆ true  │
    #   # │ [false, false] ┆ false │
    #   # │ [null]         ┆ false │
    #   # │ []             ┆ false │
    #   # │ null           ┆ null  │
    #   # └────────────────┴───────┘
    def any
      Utils.wrap_expr(_rbexpr.list_any)
    end

    # Get the length of the arrays as `:u32`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2], "bar" => [["a", "b"], ["c"]]})
    #   df.select(Polars.col("bar").list.lengths)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ bar │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # │ 1   │
    #   # └─────┘
    def len
      Utils.wrap_expr(_rbexpr.list_len)
    end
    alias_method :lengths, :len

    # Drop all null values in the list.
    #
    # The original order of the remaining elements is preserved.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[nil, 1, nil, 2], [nil], [3, 4]]})
    #   df.with_columns(drop_nulls: Polars.col("values").list.drop_nulls)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────────┬────────────┐
    #   # │ values         ┆ drop_nulls │
    #   # │ ---            ┆ ---        │
    #   # │ list[i64]      ┆ list[i64]  │
    #   # ╞════════════════╪════════════╡
    #   # │ [null, 1, … 2] ┆ [1, 2]     │
    #   # │ [null]         ┆ []         │
    #   # │ [3, 4]         ┆ [3, 4]     │
    #   # └────────────────┴────────────┘
    def drop_nulls
      Utils.wrap_expr(_rbexpr.list_drop_nulls)
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1, 2, 3], [4, 5]], "n" => [2, 1]})
    #   df.with_columns(sample: Polars.col("values").list.sample(n: Polars.col("n"), seed: 1))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────────┬─────┬───────────┐
    #   # │ values    ┆ n   ┆ sample    │
    #   # │ ---       ┆ --- ┆ ---       │
    #   # │ list[i64] ┆ i64 ┆ list[i64] │
    #   # ╞═══════════╪═════╪═══════════╡
    #   # │ [1, 2, 3] ┆ 2   ┆ [2, 3]    │
    #   # │ [4, 5]    ┆ 1   ┆ [5]       │
    #   # └───────────┴─────┴───────────┘
    def sample(n: nil, fraction: nil, with_replacement: false, shuffle: false, seed: nil)
      if !n.nil? && !fraction.nil?
        msg = "cannot specify both `n` and `fraction`"
        raise ArgumentError, msg
      end

      if !fraction.nil?
        fraction = Utils.parse_into_expression(fraction)
        return Utils.wrap_expr(
          _rbexpr.list_sample_fraction(
            fraction, with_replacement, shuffle, seed
          )
        )
      end

      n = 1 if n.nil?
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.list_sample_n(n, with_replacement, shuffle, seed))
    end

    # Sum all the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").list.sum)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 1      │
    #   # │ 5      │
    #   # └────────┘
    def sum
      Utils.wrap_expr(_rbexpr.list_sum)
    end

    # Compute the max value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").list.max)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 1      │
    #   # │ 3      │
    #   # └────────┘
    def max
      Utils.wrap_expr(_rbexpr.list_max)
    end

    # Compute the min value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").list.min)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 1      │
    #   # │ 2      │
    #   # └────────┘
    def min
      Utils.wrap_expr(_rbexpr.list_min)
    end

    # Compute the mean value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").list.mean)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ f64    │
    #   # ╞════════╡
    #   # │ 1.0    │
    #   # │ 2.5    │
    #   # └────────┘
    def mean
      Utils.wrap_expr(_rbexpr.list_mean)
    end

    # Compute the median value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[-1, 0, 1], [1, 10]]})
    #   df.with_columns(Polars.col("values").list.median.alias("median"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬────────┐
    #   # │ values     ┆ median │
    #   # │ ---        ┆ ---    │
    #   # │ list[i64]  ┆ f64    │
    #   # ╞════════════╪════════╡
    #   # │ [-1, 0, 1] ┆ 0.0    │
    #   # │ [1, 10]    ┆ 5.5    │
    #   # └────────────┴────────┘
    def median
      Utils.wrap_expr(_rbexpr.list_median)
    end

    # Compute the std value of the lists in the array.
    #
    # @param ddof [Integer]
    #   “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #   By default ddof is 1.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[-1, 0, 1], [1, 10]]})
    #   df.with_columns(Polars.col("values").list.std.alias("std"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬──────────┐
    #   # │ values     ┆ std      │
    #   # │ ---        ┆ ---      │
    #   # │ list[i64]  ┆ f64      │
    #   # ╞════════════╪══════════╡
    #   # │ [-1, 0, 1] ┆ 1.0      │
    #   # │ [1, 10]    ┆ 6.363961 │
    #   # └────────────┴──────────┘
    def std(ddof: 1)
      Utils.wrap_expr(_rbexpr.list_std(ddof))
    end

    # Compute the var value of the lists in the array.
    #
    # @param ddof [Integer]
    #   “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #   By default ddof is 1.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[-1, 0, 1], [1, 10]]})
    #   df.with_columns(Polars.col("values").list.var.alias("var"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬──────┐
    #   # │ values     ┆ var  │
    #   # │ ---        ┆ ---  │
    #   # │ list[i64]  ┆ f64  │
    #   # ╞════════════╪══════╡
    #   # │ [-1, 0, 1] ┆ 1.0  │
    #   # │ [1, 10]    ┆ 40.5 │
    #   # └────────────┴──────┘
    def var(ddof: 1)
      Utils.wrap_expr(_rbexpr.list_var(ddof))
    end

    # Sort the arrays in the list.
    #
    # @param reverse [Boolean]
    #   Sort in descending order.
    # @param nulls_last [Boolean]
    #   Place null values last.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[3, 2, 1], [9, 1, 2]]
    #     }
    #   )
    #   df.select(Polars.col("a").list.sort)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2, 3] │
    #   # │ [1, 2, 9] │
    #   # └───────────┘
    def sort(reverse: false, nulls_last: false)
      Utils.wrap_expr(_rbexpr.list_sort(reverse, nulls_last))
    end

    # Reverse the arrays in the list.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[3, 2, 1], [9, 1, 2]]
    #     }
    #   )
    #   df.select(Polars.col("a").list.reverse)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2, 3] │
    #   # │ [2, 1, 9] │
    #   # └───────────┘
    def reverse
      Utils.wrap_expr(_rbexpr.list_reverse)
    end

    # Get the unique/distinct values in the list.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 1, 2]]
    #     }
    #   )
    #   df.select(Polars.col("a").list.unique)
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
      Utils.wrap_expr(_rbexpr.list_unique(maintain_order))
    end

    # Count the number of unique values in every sub-lists.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 1, 2], [2, 3, 4]]
    #     }
    #   )
    #   df.with_columns(n_unique: Polars.col("a").list.n_unique)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────┬──────────┐
    #   # │ a         ┆ n_unique │
    #   # │ ---       ┆ ---      │
    #   # │ list[i64] ┆ u32      │
    #   # ╞═══════════╪══════════╡
    #   # │ [1, 1, 2] ┆ 2        │
    #   # │ [2, 3, 4] ┆ 3        │
    #   # └───────────┴──────────┘
    def n_unique
      Utils.wrap_expr(_rbexpr.list_n_unique)
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @param other [Object]
    #   Columns to concat into a List Series
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [["a"], ["x"]],
    #       "b" => [["b", "c"], ["y", "z"]]
    #     }
    #   )
    #   df.select(Polars.col("a").list.concat("b"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────┐
    #   # │ a               │
    #   # │ ---             │
    #   # │ list[str]       │
    #   # ╞═════════════════╡
    #   # │ ["a", "b", "c"] │
    #   # │ ["x", "y", "z"] │
    #   # └─────────────────┘
    def concat(other)
      if other.is_a?(::Array) && ![Expr, String, Series].any? { |c| other[0].is_a?(c) }
        return concat(Series.new([other]))
      end

      if !other.is_a?(::Array)
        other_list = [other]
      else
        other_list = other.dup
      end

      other_list.insert(0, Utils.wrap_expr(_rbexpr))
      Polars.concat_list(other_list)
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").list.get(0))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ foo  │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ 3    │
    #   # │ null │
    #   # │ 1    │
    #   # └──────┘
    def get(index, null_on_oob: true)
      index = Utils.parse_into_expression(index)
      Utils.wrap_expr(_rbexpr.list_get(index, null_on_oob))
    end

    # Get the value by index in the sublists.
    #
    # @return [Expr]
    def [](item)
      get(item)
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [[3, 2, 1], [], [1, 2, 3, 4, 5]]})
    #   df.with_columns(gather: Polars.col("a").list.gather([0, 4], null_on_oob: true))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────┬──────────────┐
    #   # │ a           ┆ gather       │
    #   # │ ---         ┆ ---          │
    #   # │ list[i64]   ┆ list[i64]    │
    #   # ╞═════════════╪══════════════╡
    #   # │ [3, 2, 1]   ┆ [3, null]    │
    #   # │ []          ┆ [null, null] │
    #   # │ [1, 2, … 5] ┆ [1, 5]       │
    #   # └─────────────┴──────────────┘
    def gather(indices, null_on_oob: false)
      indices = Utils.parse_into_expression(indices)
      Utils.wrap_expr(_rbexpr.list_gather(indices, null_on_oob))
    end
    alias_method :take, :gather

    # Take every n-th value start from offset in sublists.
    #
    # @param n [Integer]
    #   Gather every n-th element.
    # @param offset [Integer]
    #   Starting index.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2, 3, 4, 5], [6, 7, 8], [9, 10, 11, 12]],
    #       "n" => [2, 1, 3],
    #       "offset" => [0, 1, 0]
    #     }
    #   )
    #   df.with_columns(
    #     gather_every: Polars.col("a").list.gather_every(
    #       Polars.col("n"), Polars.col("offset")
    #     )
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌───────────────┬─────┬────────┬──────────────┐
    #   # │ a             ┆ n   ┆ offset ┆ gather_every │
    #   # │ ---           ┆ --- ┆ ---    ┆ ---          │
    #   # │ list[i64]     ┆ i64 ┆ i64    ┆ list[i64]    │
    #   # ╞═══════════════╪═════╪════════╪══════════════╡
    #   # │ [1, 2, … 5]   ┆ 2   ┆ 0      ┆ [1, 3, 5]    │
    #   # │ [6, 7, 8]     ┆ 1   ┆ 1      ┆ [7, 8]       │
    #   # │ [9, 10, … 12] ┆ 3   ┆ 0      ┆ [9, 12]      │
    #   # └───────────────┴─────┴────────┴──────────────┘
    def gather_every(
      n,
      offset = 0
    )
      n = Utils.parse_into_expression(n)
      offset = Utils.parse_into_expression(offset)
      Utils.wrap_expr(_rbexpr.list_gather_every(n, offset))
    end

    # Get the first value of the sublists.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").list.first)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ foo  │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ 3    │
    #   # │ null │
    #   # │ 1    │
    #   # └──────┘
    def first
      get(0)
    end

    # Get the last value of the sublists.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").list.last)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ foo  │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ 1    │
    #   # │ null │
    #   # │ 2    │
    #   # └──────┘
    def last
      get(-1)
    end

    # Check if sublists contain the given item.
    #
    # @param item [Object]
    #   Item that will be checked for membership
    # @param nulls_equal [Boolean]
    #   If true, treat null as a distinct value. Null values will not propagate.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").list.contains(1))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ foo   │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ true  │
    #   # │ false │
    #   # │ true  │
    #   # └───────┘
    def contains(item, nulls_equal: true)
      Utils.wrap_expr(_rbexpr.list_contains(Utils.parse_into_expression(item), nulls_equal))
    end

    # Join all string items in a sublist and place a separator between them.
    #
    # This errors if inner type of list `!= :str`.
    #
    # @param separator [String]
    #   string to separate the items with
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => [["a", "b", "c"], ["x", "y"]]})
    #   df.select(Polars.col("s").list.join(" "))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────┐
    #   # │ s     │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │ a b c │
    #   # │ x y   │
    #   # └───────┘
    def join(separator, ignore_nulls: true)
      separator = Utils.parse_into_expression(separator, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.list_join(separator, ignore_nulls))
    end

    # Retrieve the index of the minimal value in every sublist.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2], [2, 1]]
    #     }
    #   )
    #   df.select(Polars.col("a").list.arg_min)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 0   │
    #   # │ 1   │
    #   # └─────┘
    def arg_min
      Utils.wrap_expr(_rbexpr.list_arg_min)
    end

    # Retrieve the index of the maximum value in every sublist.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2], [2, 1]]
    #     }
    #   )
    #   df.select(Polars.col("a").list.arg_max)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 0   │
    #   # └─────┘
    def arg_max
      Utils.wrap_expr(_rbexpr.list_arg_max)
    end

    # Calculate the n-th discrete difference of every sublist.
    #
    # @param n [Integer]
    #   Number of slots to shift.
    # @param null_behavior ["ignore", "drop"]
    #   How to handle null values.
    #
    # @return [Expr]
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
      Utils.wrap_expr(_rbexpr.list_diff(n, null_behavior))
    end

    # Shift values by the given period.
    #
    # @param n [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [Expr]
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
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.list_shift(n))
    end

    # Slice every sublist.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the list.
    #
    # @return [Expr]
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
      offset = Utils.parse_into_expression(offset, str_as_lit: false)
      length = Utils.parse_into_expression(length, str_as_lit: false)
      Utils.wrap_expr(_rbexpr.list_slice(offset, length))
    end

    # Slice the first `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    #
    # @return [Expr]
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
      slice(0, n)
    end

    # Slice the last `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    #
    # @return [Expr]
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
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.list_tail(n))
    end

    # Returns a column with a separate row for every list element.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [[1, 2, 3], [4, 5, 6]]})
    #   df.select(Polars.col("a").list.explode)
    #   # =>
    #   # shape: (6, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # │ 4   │
    #   # │ 5   │
    #   # │ 6   │
    #   # └─────┘
    def explode
      Utils.wrap_expr(_rbexpr.explode)
    end

    # Count how often the value produced by `element` occurs.
    #
    # @param element [Expr]
    #   An expression that produces a single value
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"listcol" => [[0], [1], [1, 2, 3, 2], [1, 2, 1], [4, 4]]})
    #   df.select(Polars.col("listcol").list.count_match(2).alias("number_of_twos"))
    #   # =>
    #   # shape: (5, 1)
    #   # ┌────────────────┐
    #   # │ number_of_twos │
    #   # │ ---            │
    #   # │ u32            │
    #   # ╞════════════════╡
    #   # │ 0              │
    #   # │ 0              │
    #   # │ 2              │
    #   # │ 1              │
    #   # │ 0              │
    #   # └────────────────┘
    def count_matches(element)
      Utils.wrap_expr(_rbexpr.list_count_matches(Utils.parse_into_expression(element)))
    end
    alias_method :count_match, :count_matches

    # Convert a List column into an Array column with the same inner data type.
    #
    # @param width [Integer]
    #   Width of the resulting Array column.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [3, 4]]},
    #     schema: {"a" => Polars::List.new(Polars::Int8)}
    #   )
    #   df.with_columns(array: Polars.col("a").list.to_array(2))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌──────────┬──────────────┐
    #   # │ a        ┆ array        │
    #   # │ ---      ┆ ---          │
    #   # │ list[i8] ┆ array[i8, 2] │
    #   # ╞══════════╪══════════════╡
    #   # │ [1, 2]   ┆ [1, 2]       │
    #   # │ [3, 4]   ┆ [3, 4]       │
    #   # └──────────┴──────────────┘
    def to_array(width)
      Utils.wrap_expr(_rbexpr.list_to_array(width))
    end

    # Convert the series of type `List` to a series of type `Struct`.
    #
    # @param n_field_strategy ["first_non_null", "max_width"]
    #   Deprecated and ignored.
    # @param fields pArray
    #   If the name and number of the desired fields is known in advance
    #   a list of field names can be given, which will be assigned by index.
    #   Otherwise, to dynamically assign field names, a custom function can be
    #   used; if neither are set, fields will be `field_0, field_1 .. field_n`.
    # @param upper_bound [Object]
    #   A polars `LazyFrame` needs to know the schema at all times, so the
    #   caller must provide an upper bound of the number of struct fields that
    #   will be created; if set incorrectly, subsequent operations may fail.
    #   (For example, an `all.sum` expression will look in the current
    #   schema to determine which columns to select).
    #
    #   When operating on a `DataFrame`, the schema does not need to be
    #   tracked or pre-determined, as the result will be eagerly evaluated,
    #   so you can leave this parameter unset.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"n" => [[0, 1], [0, 1, 2]]})
    #   df.with_columns(struct: Polars.col("n").list.to_struct(upper_bound: 2))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────┬───────────┐
    #   # │ n         ┆ struct    │
    #   # │ ---       ┆ ---       │
    #   # │ list[i64] ┆ struct[2] │
    #   # ╞═══════════╪═══════════╡
    #   # │ [0, 1]    ┆ {0,1}     │
    #   # │ [0, 1, 2] ┆ {0,1}     │
    #   # └───────────┴───────────┘
    def to_struct(n_field_strategy: "first_non_null", fields: nil, upper_bound: nil)
      if !fields.is_a?(::Array)
        if fields.nil?
          fields = upper_bound.times.map { |i| "field_#{i}" }
        else
          fields = upper_bound.times.map { |i| fields.(i) }
        end
      end

      Utils.wrap_expr(_rbexpr.list_to_struct(fields))
    end

    # Run any polars expression against the lists' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.first`, or
    #   `Polars.col`
    #
    # @return [Expr]
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
      Utils.wrap_expr(_rbexpr.list_eval(expr._rbexpr))
    end

    # Run any polars aggregation expression against the lists' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.element`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [[1, nil], [42, 13], [nil, nil]]})
    #   df.with_columns(null_count: Polars.col("a").list.agg(Polars.element.null_count))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────────────┬────────────┐
    #   # │ a            ┆ null_count │
    #   # │ ---          ┆ ---        │
    #   # │ list[i64]    ┆ u32        │
    #   # ╞══════════════╪════════════╡
    #   # │ [1, null]    ┆ 1          │
    #   # │ [42, 13]     ┆ 0          │
    #   # │ [null, null] ┆ 2          │
    #   # └──────────────┴────────────┘
    #
    # @example
    #   df.with_columns(no_nulls: Polars.col("a").list.agg(Polars.element.drop_nulls))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────────────┬───────────┐
    #   # │ a            ┆ no_nulls  │
    #   # │ ---          ┆ ---       │
    #   # │ list[i64]    ┆ list[i64] │
    #   # ╞══════════════╪═══════════╡
    #   # │ [1, null]    ┆ [1]       │
    #   # │ [42, 13]     ┆ [42, 13]  │
    #   # │ [null, null] ┆ []        │
    #   # └──────────────┴───────────┘
    def agg(expr)
      Utils.wrap_expr(_rbexpr.list_agg(expr._rbexpr))
    end

    # Filter elements in each list by a boolean expression.
    #
    # @param predicate [Object]
    #   A boolean expression that is evaluated per list element.
    #   You can refer to the current element with `Polars.element`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 8, 3], "b" => [4, 5, 2]})
    #   df.with_columns(
    #     evens: Polars.concat_list("a", "b").list.filter(Polars.element % 2 == 0)
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬───────────┐
    #   # │ a   ┆ b   ┆ evens     │
    #   # │ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64 ┆ list[i64] │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ 4   ┆ [4]       │
    #   # │ 8   ┆ 5   ┆ [8]       │
    #   # │ 3   ┆ 2   ┆ [2]       │
    #   # └─────┴─────┴───────────┘
    def filter(predicate)
      Utils.wrap_expr(_rbexpr.list_filter(predicate._rbexpr))
    end

    # Compute the SET UNION between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2, 3], [], [nil, 3], [5, 6, 7]],
    #       "b" => [[2, 3, 4], [3], [3, 4, nil], [6, 8]]
    #     }
    #   )
    #   df.with_columns(
    #     union: Polars.col("a").list.set_union("b")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌───────────┬──────────────┬──────────────┐
    #   # │ a         ┆ b            ┆ union        │
    #   # │ ---       ┆ ---          ┆ ---          │
    #   # │ list[i64] ┆ list[i64]    ┆ list[i64]    │
    #   # ╞═══════════╪══════════════╪══════════════╡
    #   # │ [1, 2, 3] ┆ [2, 3, 4]    ┆ [1, 2, … 4]  │
    #   # │ []        ┆ [3]          ┆ [3]          │
    #   # │ [null, 3] ┆ [3, 4, null] ┆ [null, 3, 4] │
    #   # │ [5, 6, 7] ┆ [6, 8]       ┆ [5, 6, … 8]  │
    #   # └───────────┴──────────────┴──────────────┘
    def set_union(other)
      if other.respond_to?(:each)
        if !other.is_a?(::Array) && !other.is_a?(Series) && !other.is_a?(DataFrame)
          other = other.to_a
        end
        other = F.lit(other)._rbexpr
      else
        other = Utils.parse_into_expression(other)
      end
      Utils.wrap_expr(_rbexpr.list_set_operation(other, "union"))
    end

    # Compute the SET DIFFERENCE between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2, 3], [], [nil, 3], [5, 6, 7]],
    #       "b" => [[2, 3, 4], [3], [3, 4, nil], [6, 8]]
    #     }
    #   )
    #   df.with_columns(difference: Polars.col("a").list.set_difference("b"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌───────────┬──────────────┬────────────┐
    #   # │ a         ┆ b            ┆ difference │
    #   # │ ---       ┆ ---          ┆ ---        │
    #   # │ list[i64] ┆ list[i64]    ┆ list[i64]  │
    #   # ╞═══════════╪══════════════╪════════════╡
    #   # │ [1, 2, 3] ┆ [2, 3, 4]    ┆ [1]        │
    #   # │ []        ┆ [3]          ┆ []         │
    #   # │ [null, 3] ┆ [3, 4, null] ┆ []         │
    #   # │ [5, 6, 7] ┆ [6, 8]       ┆ [5, 7]     │
    #   # └───────────┴──────────────┴────────────┘
    def set_difference(other)
      if other.respond_to?(:each)
        if !other.is_a?(::Array) && !other.is_a?(Series) && !other.is_a?(DataFrame)
          other = other.to_a
        end
        other = F.lit(other)._rbexpr
      else
        other = Utils.parse_into_expression(other)
      end
      Utils.wrap_expr(_rbexpr.list_set_operation(other, "difference"))
    end

    # Compute the SET INTERSECTION between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2, 3], [], [nil, 3], [5, 6, 7]],
    #       "b" => [[2, 3, 4], [3], [3, 4, nil], [6, 8]]
    #     }
    #   )
    #   df.with_columns(intersection: Polars.col("a").list.set_intersection("b"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌───────────┬──────────────┬──────────────┐
    #   # │ a         ┆ b            ┆ intersection │
    #   # │ ---       ┆ ---          ┆ ---          │
    #   # │ list[i64] ┆ list[i64]    ┆ list[i64]    │
    #   # ╞═══════════╪══════════════╪══════════════╡
    #   # │ [1, 2, 3] ┆ [2, 3, 4]    ┆ [2, 3]       │
    #   # │ []        ┆ [3]          ┆ []           │
    #   # │ [null, 3] ┆ [3, 4, null] ┆ [null, 3]    │
    #   # │ [5, 6, 7] ┆ [6, 8]       ┆ [6]          │
    #   # └───────────┴──────────────┴──────────────┘
    def set_intersection(other)
      if other.respond_to?(:each)
        if !other.is_a?(::Array) && !other.is_a?(Series) && !other.is_a?(DataFrame)
          other = other.to_a
        end
        other = F.lit(other)._rbexpr
      else
        other = Utils.parse_into_expression(other)
      end
      Utils.wrap_expr(_rbexpr.list_set_operation(other, "intersection"))
    end

    # Compute the SET SYMMETRIC DIFFERENCE between the elements in this list and the elements of `other`.
    #
    # @param other [Object]
    #   Right hand side of the set operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2, 3], [], [nil, 3], [5, 6, 7]],
    #       "b" => [[2, 3, 4], [3], [3, 4, nil], [6, 8]]
    #     }
    #   )
    #   df.with_columns(sdiff: Polars.col("b").list.set_symmetric_difference("a"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌───────────┬──────────────┬───────────┐
    #   # │ a         ┆ b            ┆ sdiff     │
    #   # │ ---       ┆ ---          ┆ ---       │
    #   # │ list[i64] ┆ list[i64]    ┆ list[i64] │
    #   # ╞═══════════╪══════════════╪═══════════╡
    #   # │ [1, 2, 3] ┆ [2, 3, 4]    ┆ [4, 1]    │
    #   # │ []        ┆ [3]          ┆ [3]       │
    #   # │ [null, 3] ┆ [3, 4, null] ┆ [4]       │
    #   # │ [5, 6, 7] ┆ [6, 8]       ┆ [8, 5, 7] │
    #   # └───────────┴──────────────┴───────────┘
    def set_symmetric_difference(other)
      if other.respond_to?(:each)
        if !other.is_a?(::Array) && !other.is_a?(Series) && !other.is_a?(DataFrame)
          other = other.to_a
        end
        other = F.lit(other)._rbexpr
      else
        other = Utils.parse_into_expression(other)
      end
      Utils.wrap_expr(_rbexpr.list_set_operation(other, "symmetric_difference"))
    end
  end
end

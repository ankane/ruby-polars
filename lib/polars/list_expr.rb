module Polars
  # Namespace for list related expressions.
  class ListExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Get the length of the arrays as `:u32`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2], "bar" => [["a", "b"], ["c"]]})
    #   df.select(Polars.col("bar").arr.lengths)
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
    def lengths
      Utils.wrap_expr(_rbexpr.arr_lengths)
    end

    # Sum all the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").arr.sum)
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
      Utils.wrap_expr(_rbexpr.lst_sum)
    end

    # Compute the max value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").arr.max)
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
      Utils.wrap_expr(_rbexpr.lst_max)
    end

    # Compute the min value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").arr.min)
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
      Utils.wrap_expr(_rbexpr.lst_min)
    end

    # Compute the mean value of the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").arr.mean)
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
      Utils.wrap_expr(_rbexpr.lst_mean)
    end

    # Sort the arrays in the list.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[3, 2, 1], [9, 1, 2]]
    #     }
    #   )
    #   df.select(Polars.col("a").arr.sort)
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
    def sort(reverse: false)
      Utils.wrap_expr(_rbexpr.lst_sort(reverse))
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
    #   df.select(Polars.col("a").arr.reverse)
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
      Utils.wrap_expr(_rbexpr.lst_reverse)
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
    def unique
      Utils.wrap_expr(_rbexpr.lst_unique)
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
    #   df.select(Polars.col("a").arr.concat("b"))
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
      if other.is_a?(Array) && ![Expr, String, Series].any? { |c| other[0].is_a?(c) }
        return concat(Series.new([other]))
      end

      if !other.is_a?(Array)
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
    # if an index is out of bounds, it will return a `None`.
    #
    # @param index [Integer]
    #   Index to return per sublist
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").arr.get(0))
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
    def get(index)
      index = Utils.expr_to_lit_or_expr(index, str_to_lit: false)._rbexpr
      Utils.wrap_expr(_rbexpr.lst_get(index))
    end

    # Get the value by index in the sublists.
    #
    # @return [Expr]
    def [](item)
      get(item)
    end

    # Get the first value of the sublists.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").arr.first)
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
    #   df.select(Polars.col("foo").arr.last)
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
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [[3, 2, 1], [], [1, 2]]})
    #   df.select(Polars.col("foo").arr.contains(1))
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
    def contains(item)
      Utils.wrap_expr(_rbexpr.arr_contains(Utils.expr_to_lit_or_expr(item)._rbexpr))
    end

    # Join all string items in a sublist and place a separator between them.
    #
    # This errors if inner type of list `!= :str`.
    #
    # @param separator [String]
    #   string to separate the items with
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => [["a", "b", "c"], ["x", "y"]]})
    #   df.select(Polars.col("s").arr.join(" "))
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
    def join(separator)
      Utils.wrap_expr(_rbexpr.lst_join(separator))
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
    #   df.select(Polars.col("a").arr.arg_min)
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
      Utils.wrap_expr(_rbexpr.lst_arg_min)
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
    #   df.select(Polars.col("a").arr.arg_max)
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
      Utils.wrap_expr(_rbexpr.lst_arg_max)
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
    #   s.arr.diff
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [null, 1, ... 1]
    #   #         [null, -8, -1]
    #   # ]
    def diff(n: 1, null_behavior: "ignore")
      Utils.wrap_expr(_rbexpr.lst_diff(n, null_behavior))
    end

    # Shift values by the given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [Expr]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.shift
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [null, 1, ... 3]
    #   #         [null, 10, 2]
    #   # ]
    def shift(periods = 1)
      Utils.wrap_expr(_rbexpr.lst_shift(periods))
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
    #   s.arr.slice(1, 2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [2, 3]
    #   #         [2, 1]
    #   # ]
    def slice(offset, length = nil)
      offset = Utils.expr_to_lit_or_expr(offset, str_to_lit: false)._rbexpr
      length = Utils.expr_to_lit_or_expr(length, str_to_lit: false)._rbexpr
      Utils.wrap_expr(_rbexpr.lst_slice(offset, length))
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
    #   s.arr.head(2)
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
    #   s.arr.tail(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list[i64]]
    #   # [
    #   #         [3, 4]
    #   #         [2, 1]
    #   # ]
    def tail(n = 5)
      offset = -Utils.expr_to_lit_or_expr(n, str_to_lit: false)
      slice(offset, n)
    end

    # Convert the series of type `List` to a series of type `Struct`.
    #
    # @param n_field_strategy ["first_non_null", "max_width"]
    #   Strategy to determine the number of fields of the struct.
    # @param name_generator [Object]
    #   A custom function that can be used to generate the field names.
    #   Default field names are `field_0, field_1 .. field_n`
    #
    # @return [Expr]
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
      raise Todo if name_generator
      Utils.wrap_expr(_rbexpr.lst_to_struct(n_field_strategy, name_generator, 0))
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
    # @return [Expr]
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
       Utils.wrap_expr(_rbexpr.lst_eval(expr._rbexpr, parallel))
    end
  end
end

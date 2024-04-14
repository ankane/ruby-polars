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

    # Get the unique/distinct values in the array.
    #
    # @param maintain_order [Boolean]
    #   Maintain order of data. This requires more work.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 1, 2]]
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
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
      Utils.wrap_expr(_rbexpr.arr_unique(maintain_order))
    end

    # Convert an Array column into a List column with the same inner data type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [3, 4]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int8, 2)}
    #   )
    #   df.select(Polars.col("a").arr.to_list)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ list[i8] │
    #   # ╞══════════╡
    #   # │ [1, 2]   │
    #   # │ [3, 4]   │
    #   # └──────────┘
    def to_list
      Utils.wrap_expr(_rbexpr.arr_to_list)
    end

    # Evaluate whether any boolean value is true for every subarray.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a": [
    #         [true, true],
    #         [false, true],
    #         [false, false],
    #         [nil, nil],
    #         nil
    #       ]
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Boolean, 2)}
    #   )
    #   df.with_columns(any: Polars.col("a").arr.any)
    #   # =>
    #   # shape: (5, 2)
    #   # ┌────────────────┬───────┐
    #   # │ a              ┆ any   │
    #   # │ ---            ┆ ---   │
    #   # │ array[bool, 2] ┆ bool  │
    #   # ╞════════════════╪═══════╡
    #   # │ [true, true]   ┆ true  │
    #   # │ [false, true]  ┆ true  │
    #   # │ [false, false] ┆ false │
    #   # │ [null, null]   ┆ false │
    #   # │ null           ┆ null  │
    #   # └────────────────┴───────┘
    def any
      Utils.wrap_expr(_rbexpr.arr_any)
    end

    # Evaluate whether all boolean values are true for every subarray.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a": [
    #         [true, true],
    #         [false, true],
    #         [false, false],
    #         [nil, nil],
    #         nil
    #       ]
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Boolean, 2)}
    #   )
    #   df.with_columns(all: Polars.col("a").arr.all)
    #   # =>
    #   # shape: (5, 2)
    #   # ┌────────────────┬───────┐
    #   # │ a              ┆ all   │
    #   # │ ---            ┆ ---   │
    #   # │ array[bool, 2] ┆ bool  │
    #   # ╞════════════════╪═══════╡
    #   # │ [true, true]   ┆ true  │
    #   # │ [false, true]  ┆ false │
    #   # │ [false, false] ┆ false │
    #   # │ [null, null]   ┆ true  │
    #   # │ null           ┆ null  │
    #   # └────────────────┴───────┘
    def all
      Utils.wrap_expr(_rbexpr.arr_all)
    end

    # Sort the arrays in this column.
    #
    # @param descending [Boolean]
    #   Sort in descending order.
    # @param nulls_last [Boolean]
    #   Place null values last.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[3, 2, 1], [9, 1, 2]],
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.with_columns(sort: Polars.col("a").arr.sort)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬───────────────┐
    #   # │ a             ┆ sort          │
    #   # │ ---           ┆ ---           │
    #   # │ array[i64, 3] ┆ array[i64, 3] │
    #   # ╞═══════════════╪═══════════════╡
    #   # │ [3, 2, 1]     ┆ [1, 2, 3]     │
    #   # │ [9, 1, 2]     ┆ [1, 2, 9]     │
    #   # └───────────────┴───────────────┘
    #
    # @example
    #   df.with_columns(sort: Polars.col("a").arr.sort(descending: true))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬───────────────┐
    #   # │ a             ┆ sort          │
    #   # │ ---           ┆ ---           │
    #   # │ array[i64, 3] ┆ array[i64, 3] │
    #   # ╞═══════════════╪═══════════════╡
    #   # │ [3, 2, 1]     ┆ [3, 2, 1]     │
    #   # │ [9, 1, 2]     ┆ [9, 2, 1]     │
    #   # └───────────────┴───────────────┘
    def sort(descending: false, nulls_last: false)
      Utils.wrap_expr(_rbexpr.arr_sort(descending, nulls_last))
    end

    # Reverse the arrays in this column.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[3, 2, 1], [9, 1, 2]]
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.with_columns(reverse: Polars.col("a").arr.reverse)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬───────────────┐
    #   # │ a             ┆ reverse       │
    #   # │ ---           ┆ ---           │
    #   # │ array[i64, 3] ┆ array[i64, 3] │
    #   # ╞═══════════════╪═══════════════╡
    #   # │ [3, 2, 1]     ┆ [1, 2, 3]     │
    #   # │ [9, 1, 2]     ┆ [2, 1, 9]     │
    #   # └───────────────┴───────────────┘
    def reverse
      Utils.wrap_expr(_rbexpr.arr_reverse)
    end

    # Retrieve the index of the minimal value in every sub-array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2], [2, 1]]
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.with_columns(arg_min: Polars.col("a").arr.arg_min)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬─────────┐
    #   # │ a             ┆ arg_min │
    #   # │ ---           ┆ ---     │
    #   # │ array[i64, 2] ┆ u32     │
    #   # ╞═══════════════╪═════════╡
    #   # │ [1, 2]        ┆ 0       │
    #   # │ [2, 1]        ┆ 1       │
    #   # └───────────────┴─────────┘
    def arg_min
      Utils.wrap_expr(_rbexpr.arr_arg_min)
    end

    # Retrieve the index of the maximum value in every sub-array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 2], [2, 1]]
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.with_columns(arg_max: Polars.col("a").arr.arg_max)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬─────────┐
    #   # │ a             ┆ arg_max │
    #   # │ ---           ┆ ---     │
    #   # │ array[i64, 2] ┆ u32     │
    #   # ╞═══════════════╪═════════╡
    #   # │ [1, 2]        ┆ 1       │
    #   # │ [2, 1]        ┆ 0       │
    #   # └───────────────┴─────────┘
    def arg_max
      Utils.wrap_expr(_rbexpr.arr_arg_max)
    end

    # Get the value by index in the sub-arrays.
    #
    # So index `0` would return the first item of every sublist
    # and index `-1` would return the last item of every sublist
    # if an index is out of bounds, it will return a `nil`.
    #
    # @param index [Integer]
    #   Index to return per sub-array
    # @param null_on_oob [Boolean]
    #   Behavior if an index is out of bounds:
    #   true -> set as null
    #   false -> raise an error
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"arr" => [[1, 2, 3], [4, 5, 6], [7, 8, 9]], "idx" => [1, -2, 4]},
    #     schema: {"arr" => Polars::Array.new(Polars::Int32, 3), "idx" => Polars::Int32}
    #   )
    #   df.with_columns(get: Polars.col("arr").arr.get("idx"))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────────────┬─────┬──────┐
    #   # │ arr           ┆ idx ┆ get  │
    #   # │ ---           ┆ --- ┆ ---  │
    #   # │ array[i32, 3] ┆ i32 ┆ i32  │
    #   # ╞═══════════════╪═════╪══════╡
    #   # │ [1, 2, 3]     ┆ 1   ┆ 2    │
    #   # │ [4, 5, 6]     ┆ -2  ┆ 5    │
    #   # │ [7, 8, 9]     ┆ 4   ┆ null │
    #   # └───────────────┴─────┴──────┘
    def get(index, null_on_oob: true)
      index = Utils.parse_as_expression(index)
      Utils.wrap_expr(_rbexpr.arr_get(index, null_on_oob))
    end

    # Get the first value of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2, 3], [4, 5, 6], [7, 8, 9]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int32, 3)}
    #   )
    #   df.with_columns(first: Polars.col("a").arr.first)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬───────┐
    #   # │ a             ┆ first │
    #   # │ ---           ┆ ---   │
    #   # │ array[i32, 3] ┆ i32   │
    #   # ╞═══════════════╪═══════╡
    #   # │ [1, 2, 3]     ┆ 1     │
    #   # │ [4, 5, 6]     ┆ 4     │
    #   # │ [7, 8, 9]     ┆ 7     │
    #   # └───────────────┴───────┘
    def first
      get(0)
    end

    # Get the last value of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2, 3], [4, 5, 6], [7, 8, 9]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int32, 3)}
    #   )
    #   df.with_columns(last: Polars.col("a").arr.last)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬──────┐
    #   # │ a             ┆ last │
    #   # │ ---           ┆ ---  │
    #   # │ array[i32, 3] ┆ i32  │
    #   # ╞═══════════════╪══════╡
    #   # │ [1, 2, 3]     ┆ 3    │
    #   # │ [4, 5, 6]     ┆ 6    │
    #   # │ [7, 8, 9]     ┆ 9    │
    #   # └───────────────┴──────┘
    def last
      get(-1)
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"s" => [["a", "b"], ["x", "y"]], "separator" => ["*", "_"]},
    #     schema: {
    #       "s" => Polars::Array.new(Polars::String, 2),
    #       "separator" => Polars::String
    #     }
    #   )
    #   df.with_columns(join: Polars.col("s").arr.join(Polars.col("separator")))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────────────┬───────────┬──────┐
    #   # │ s             ┆ separator ┆ join │
    #   # │ ---           ┆ ---       ┆ ---  │
    #   # │ array[str, 2] ┆ str       ┆ str  │
    #   # ╞═══════════════╪═══════════╪══════╡
    #   # │ ["a", "b"]    ┆ *         ┆ a*b  │
    #   # │ ["x", "y"]    ┆ _         ┆ x_y  │
    #   # └───────────────┴───────────┴──────┘
    def join(separator, ignore_nulls: true)
      separator = Utils.parse_as_expression(separator, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.arr_join(separator, ignore_nulls))
    end

    # Returns a column with a separate row for every array element.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2, 3], [4, 5, 6]]}, schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.select(Polars.col("a").arr.explode)
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

    # Check if sub-arrays contain the given item.
    #
    # @param item [Object]
    #   Item that will be checked for membership
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [["a", "b"], ["x", "y"], ["a", "c"]]},
    #     schema: {"a" => Polars::Array.new(Polars::String, 2)}
    #   )
    #   df.with_columns(contains: Polars.col("a").arr.contains("a"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬──────────┐
    #   # │ a             ┆ contains │
    #   # │ ---           ┆ ---      │
    #   # │ array[str, 2] ┆ bool     │
    #   # ╞═══════════════╪══════════╡
    #   # │ ["a", "b"]    ┆ true     │
    #   # │ ["x", "y"]    ┆ false    │
    #   # │ ["a", "c"]    ┆ true     │
    #   # └───────────────┴──────────┘
    def contains(item)
      item = Utils.parse_as_expression(item, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.arr_contains(item))
    end

    # Count how often the value produced by `element` occurs.
    #
    # @param element [Object]
    #   An expression that produces a single value
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [1, 1], [2, 2]]}, schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.with_columns(number_of_twos: Polars.col("a").arr.count_matches(2))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬────────────────┐
    #   # │ a             ┆ number_of_twos │
    #   # │ ---           ┆ ---            │
    #   # │ array[i64, 2] ┆ u32            │
    #   # ╞═══════════════╪════════════════╡
    #   # │ [1, 2]        ┆ 1              │
    #   # │ [1, 1]        ┆ 0              │
    #   # │ [2, 2]        ┆ 2              │
    #   # └───────────────┴────────────────┘
    def count_matches(element)
      element = Utils.parse_as_expression(element, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.arr_count_matches(element))
    end
  end
end

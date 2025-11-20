module Polars
  # Namespace for array related expressions.
  class ArrayExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Return the number of elements in each array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.len)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # │ 2   │
    #   # └─────┘
    def len
      Utils.wrap_expr(_rbexpr.arr_len)
    end

    # Slice every subarray.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the list.
    # @param as_array [Boolean]
    #   Return result as a fixed-length `Array`, otherwise as a `List`.
    #   If true `length` and `offset` must be constant values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.slice(0, 1))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1]       │
    #   # │ [4]       │
    #   # └───────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.slice(0, 1, as_array: true))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────────┐
    #   # │ a             │
    #   # │ ---           │
    #   # │ array[i64, 1] │
    #   # ╞═══════════════╡
    #   # │ [1]           │
    #   # │ [4]           │
    #   # └───────────────┘
    def slice(
      offset,
      length = nil,
      as_array: false
    )
      offset = Utils.parse_into_expression(offset)
      length = !length.nil? ? Utils.parse_into_expression(length) : nil
      Utils.wrap_expr(_rbexpr.arr_slice(offset, length, as_array))
    end

    # Get the first `n` elements of the sub-arrays.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    # @param as_array [Boolean]
    #   Return result as a fixed-length `Array`, otherwise as a `List`.
    #   If true `n` must be a constant value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.head(1))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1]       │
    #   # │ [4]       │
    #   # └───────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.head(1, as_array: true))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────────┐
    #   # │ a             │
    #   # │ ---           │
    #   # │ array[i64, 1] │
    #   # ╞═══════════════╡
    #   # │ [1]           │
    #   # │ [4]           │
    #   # └───────────────┘
    def head(n = 5, as_array: false)
      slice(0, n, as_array: as_array)
    end

    # Slice the last `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    # @param as_array [Boolean]
    #   Return result as a fixed-length `Array`, otherwise as a `List`.
    #   If true `n` must be a constant value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.tail(1))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [2]       │
    #   # │ [3]       │
    #   # └───────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 2)}
    #   )
    #   df.select(Polars.col("a").arr.tail(1, as_array: true))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────────┐
    #   # │ a             │
    #   # │ ---           │
    #   # │ array[i64, 1] │
    #   # ╞═══════════════╡
    #   # │ [2]           │
    #   # │ [3]           │
    #   # └───────────────┘
    def tail(n = 5, as_array: false)
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.arr_tail(n, as_array))
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
      Utils.wrap_expr(_rbexpr.arr_min)
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
      Utils.wrap_expr(_rbexpr.arr_max)
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
      Utils.wrap_expr(_rbexpr.arr_sum)
    end

    # Compute the std of the values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(2, Polars::Int64)}
    #   )
    #   df.select(Polars.col("a").arr.std)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.707107 │
    #   # │ 0.707107 │
    #   # └──────────┘
    def std(ddof: 1)
      Utils.wrap_expr(_rbexpr.arr_std(ddof))
    end

    # Compute the var of the values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(2, Polars::Int64)}
    #   )
    #   df.select(Polars.col("a").arr.var)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.5 │
    #   # │ 0.5 │
    #   # └─────┘
    def var(ddof: 1)
      Utils.wrap_expr(_rbexpr.arr_var(ddof))
    end

    # Compute the mean of the values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2, 3], [1, 1, 16]]},
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.select(Polars.col("a").arr.mean)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 2.0 │
    #   # │ 6.0 │
    #   # └─────┘
    def mean
      Utils.wrap_expr(_rbexpr.arr_mean)
    end

    # Compute the median of the values of the sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2], [4, 3]]},
    #     schema: {"a" => Polars::Array.new(2, Polars::Int64)}
    #   )
    #   df.select(Polars.col("a").arr.median)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.5 │
    #   # │ 3.5 │
    #   # └─────┘
    def median
      Utils.wrap_expr(_rbexpr.arr_median)
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

    # Count the number of unique values in every sub-arrays.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [[1, 1, 2], [2, 3, 4]],
    #     },
    #     schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.with_columns(n_unique: Polars.col("a").arr.n_unique)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬──────────┐
    #   # │ a             ┆ n_unique │
    #   # │ ---           ┆ ---      │
    #   # │ array[i64, 3] ┆ u32      │
    #   # ╞═══════════════╪══════════╡
    #   # │ [1, 1, 2]     ┆ 2        │
    #   # │ [2, 3, 4]     ┆ 3        │
    #   # └───────────────┴──────────┘
    def n_unique
      Utils.wrap_expr(_rbexpr.arr_n_unique)
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
    #   df.with_columns(get: Polars.col("arr").arr.get("idx", null_on_oob: true))
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
    def get(index, null_on_oob: false)
      index = Utils.parse_into_expression(index)
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
      get(0, null_on_oob: true)
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
      get(-1, null_on_oob: true)
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
      separator = Utils.parse_into_expression(separator, str_as_lit: true)
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
    # @param nulls_equal [Boolean]
    #   If true, treat null as a distinct value. Null values will not propagate.
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
    def contains(item, nulls_equal: true)
      item = Utils.parse_into_expression(item, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.arr_contains(item, nulls_equal))
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
      element = Utils.parse_into_expression(element, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.arr_count_matches(element))
    end

    # Convert the Series of type `Array` to a Series of type `Struct`.
    #
    # @param fields [Object]
    #   If the name and number of the desired fields is known in advance
    #   a list of field names can be given, which will be assigned by index.
    #   Otherwise, to dynamically assign field names, a custom function can be
    #   used; if neither are set, fields will be `field_0, field_1 .. field_n`.
    #
    # @return [Expr]
    #
    # @example Convert array to struct with default field name assignment:
    #   df = Polars::DataFrame.new(
    #     {"n" => [[0, 1, 2], [3, 4, 5]]}, schema: {"n" => Polars::Array.new(Polars::Int8, 3)}
    #   )
    #   df.with_columns(struct: Polars.col("n").arr.to_struct)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌──────────────┬───────────┐
    #   # │ n            ┆ struct    │
    #   # │ ---          ┆ ---       │
    #   # │ array[i8, 3] ┆ struct[3] │
    #   # ╞══════════════╪═══════════╡
    #   # │ [0, 1, 2]    ┆ {0,1,2}   │
    #   # │ [3, 4, 5]    ┆ {3,4,5}   │
    #   # └──────────────┴───────────┘
    def to_struct(fields: nil)
      raise Todo if fields
      if fields.is_a?(Enumerable)
        field_names = fields.to_a
        rbexpr = _rbexpr.arr_to_struct(nil)
        Utils.wrap_expr(rbexpr).struct.rename_fields(field_names)
      else
        rbexpr = _rbexpr.arr_to_struct(fields)
        Utils.wrap_expr(rbexpr)
      end
    end

    # Shift array values by the given number of indices.
    #
    # @param n [Integer]
    #   Number of indices to shift forward. If a negative value is passed, values
    #   are shifted in the opposite direction instead.
    #
    # @return [Expr]
    #
    # @note
    #   This method is similar to the `LAG` operation in SQL when the value for `n`
    #   is positive. With a negative value for `n`, it is similar to `LEAD`.
    #
    # @example By default, array values are shifted forward by one index.
    #   df = Polars::DataFrame.new(
    #     {"a" => [[1, 2, 3], [4, 5, 6]]}, schema: {"a" => Polars::Array.new(Polars::Int64, 3)}
    #   )
    #   df.with_columns(shift: Polars.col("a").arr.shift)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬───────────────┐
    #   # │ a             ┆ shift         │
    #   # │ ---           ┆ ---           │
    #   # │ array[i64, 3] ┆ array[i64, 3] │
    #   # ╞═══════════════╪═══════════════╡
    #   # │ [1, 2, 3]     ┆ [null, 1, 2]  │
    #   # │ [4, 5, 6]     ┆ [null, 4, 5]  │
    #   # └───────────────┴───────────────┘
    #
    # @example Pass a negative value to shift in the opposite direction instead.
    #   df.with_columns(shift: Polars.col("a").arr.shift(-2))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────────┬─────────────────┐
    #   # │ a             ┆ shift           │
    #   # │ ---           ┆ ---             │
    #   # │ array[i64, 3] ┆ array[i64, 3]   │
    #   # ╞═══════════════╪═════════════════╡
    #   # │ [1, 2, 3]     ┆ [3, null, null] │
    #   # │ [4, 5, 6]     ┆ [6, null, null] │
    #   # └───────────────┴─────────────────┘
    def shift(n = 1)
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.arr_shift(n))
    end

    # Run any polars expression against the arrays' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.element`
    # @param as_list [Boolean]
    #   Collect the resulting data as a list. This allows for expressions which
    #   output a variable amount of data.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 8, 3], "b" => [4, 5, 2]})
    #   df.with_columns(rank: Polars.concat_arr("a", "b").arr.eval(Polars.element.rank))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬───────────────┐
    #   # │ a   ┆ b   ┆ rank          │
    #   # │ --- ┆ --- ┆ ---           │
    #   # │ i64 ┆ i64 ┆ array[f64, 2] │
    #   # ╞═════╪═════╪═══════════════╡
    #   # │ 1   ┆ 4   ┆ [1.0, 2.0]    │
    #   # │ 8   ┆ 5   ┆ [2.0, 1.0]    │
    #   # │ 3   ┆ 2   ┆ [2.0, 1.0]    │
    #   # └─────┴─────┴───────────────┘
    def eval(expr, as_list: false)
      Utils.wrap_expr(_rbexpr.arr_eval(expr._rbexpr, as_list))
    end

    # Run any polars aggregation expression against the arrays' elements.
    #
    # @param expr [Expr]
    #   Expression to run. Note that you can select an element with `Polars.element`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::Series.new(
    #     "a", [[1, nil], [42, 13], [nil, nil]], dtype: Polars::Array.new(Polars::Int64, 2)
    #   ).to_frame
    #   df.with_columns(null_count: Polars.col("a").arr.agg(Polars.element.null_count))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬────────────┐
    #   # │ a             ┆ null_count │
    #   # │ ---           ┆ ---        │
    #   # │ array[i64, 2] ┆ u32        │
    #   # ╞═══════════════╪════════════╡
    #   # │ [1, null]     ┆ 1          │
    #   # │ [42, 13]      ┆ 0          │
    #   # │ [null, null]  ┆ 2          │
    #   # └───────────────┴────────────┘
    #
    # @example
    #   df.with_columns(no_nulls: Polars.col("a").arr.agg(Polars.element.drop_nulls))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬───────────┐
    #   # │ a             ┆ no_nulls  │
    #   # │ ---           ┆ ---       │
    #   # │ array[i64, 2] ┆ list[i64] │
    #   # ╞═══════════════╪═══════════╡
    #   # │ [1, null]     ┆ [1]       │
    #   # │ [42, 13]      ┆ [42, 13]  │
    #   # │ [null, null]  ┆ []        │
    #   # └───────────────┴───────────┘
    def agg(expr)
      Utils.wrap_expr(_rbexpr.arr_agg(expr._rbexpr))
    end
  end
end

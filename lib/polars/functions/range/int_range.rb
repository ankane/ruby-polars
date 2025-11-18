module Polars
  module Functions
    # Create a range expression (or Series).
    #
    # This can be used in a `select`, `with_column`, etc. Be sure that the resulting
    # range size is equal to the length of the DataFrame you are collecting.
    #
    # @param start [Integer, Expr, Series]
    #   Lower bound of range.
    # @param stop [Integer, Expr, Series]
    #   Upper bound of range.
    # @param step [Integer]
    #   Step size of the range.
    # @param eager [Boolean]
    #   If eager evaluation is `True`, a Series is returned instead of an Expr.
    # @param dtype [Symbol]
    #   Apply an explicit integer dtype to the resulting expression (default is `Int64`).
    #
    # @return [Expr, Series]
    #
    # @example
    #   Polars.arange(0, 3, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'arange' [i64]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   # ]
    def int_range(start = 0, stop = nil, step: 1, eager: false, dtype: nil)
      if stop.nil?
        stop = start
        start = 0
      end

      start = Utils.parse_into_expression(start)
      stop = Utils.parse_into_expression(stop)
      dtype ||= Int64
      dtype = dtype.to_s if dtype.is_a?(Symbol)
      result = Utils.wrap_expr(Plr.int_range(start, stop, step, dtype)).alias("arange")

      if eager
        return select(result).to_series
      end

      result
    end
    alias_method :arange, :int_range

    # Generate a range of integers for each row of the input columns.
    #
    # @param start [Integer, Expr, Series]
    #   Start of the range (inclusive). Defaults to 0.
    # @param stop [Integer, Expr, Series]
    #   End of the range (exclusive). If set to `None` (default),
    #   the value of `start` is used and `start` is set to `0`.
    # @param step [Integer]
    #   Step size of the range.
    # @param dtype [Object]
    #   Integer data type of the ranges. Defaults to `Int64`.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Expr, Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"start" => [1, -1], "end" => [3, 2]})
    #   df.with_columns(int_range: Polars.int_ranges("start", "end"))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────┬─────┬────────────┐
    #   # │ start ┆ end ┆ int_range  │
    #   # │ ---   ┆ --- ┆ ---        │
    #   # │ i64   ┆ i64 ┆ list[i64]  │
    #   # ╞═══════╪═════╪════════════╡
    #   # │ 1     ┆ 3   ┆ [1, 2]     │
    #   # │ -1    ┆ 2   ┆ [-1, 0, 1] │
    #   # └───────┴─────┴────────────┘
    #
    # @example `end` can be omitted for a shorter syntax.
    #   df.select("end", int_range: Polars.int_ranges("end"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬───────────┐
    #   # │ end ┆ int_range │
    #   # │ --- ┆ ---       │
    #   # │ i64 ┆ list[i64] │
    #   # ╞═════╪═══════════╡
    #   # │ 3   ┆ [0, 1, 2] │
    #   # │ 2   ┆ [0, 1]    │
    #   # └─────┴───────────┘
    def int_ranges(
      start = 0,
      stop = nil,
      step: 1,
      dtype: Int64,
      eager: false
    )
      if stop.nil?
        stop = start
        start = 0
      end

      dtype_expr = Utils.parse_into_datatype_expr(dtype)
      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)
      step_rbexpr = Utils.parse_into_expression(step)
      result = Utils.wrap_expr(
        Plr.int_ranges(
          start_rbexpr, end_rbexpr, step_rbexpr, dtype_expr._rbdatatype_expr
        )
      )

      if eager
        return F.select(result).to_series
      end

      result
    end
  end
end

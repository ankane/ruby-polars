module Polars
  module Functions
    # Create sequence of evenly-spaced points.
    #
    # @param start [Object]
    #   Lower bound of the range.
    # @param stop [Object]
    #   Upper bound of the range.
    # @param num_samples [Object]
    #   Number of samples in the output sequence.
    # @param closed ['both', 'left', 'right', 'none']
    #   Define which sides of the interval are closed (inclusive).
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Object]
    #
    # @note
    #   This functionality is experimental. It may be changed at any point without it
    #   being considered a breaking change.
    #
    # @example
    #   Polars.linear_space(0, 1, 3, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'literal' [f64]
    #   # [
    #   #         0.0
    #   #         0.5
    #   #         1.0
    #   # ]
    #
    # @example
    #   Polars.linear_space(0, 1, 3, closed: "left", eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'literal' [f64]
    #   # [
    #   #         0.0
    #   #         0.333333
    #   #         0.666667
    #   # ]
    #
    # @example
    #   Polars.linear_space(0, 1, 3, closed: "right", eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'literal' [f64]
    #   # [
    #   #         0.333333
    #   #         0.666667
    #   #         1.0
    #   # ]
    #
    # @example
    #   Polars.linear_space(0, 1, 3, closed: "none", eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'literal' [f64]
    #   # [
    #   #         0.25
    #   #         0.5
    #   #         0.75
    #   # ]
    #
    # @example `Date` endpoints generate a sequence of `Datetime` values:
    #   Polars.linear_space(
    #     Date.new(2025, 1, 1),
    #     Date.new(2025, 2, 1),
    #     3,
    #     closed: "right",
    #     eager: true
    #   )
    #   # =>
    #   # shape: (3,)
    #   # Series: 'literal' [datetime[μs]]
    #   # [
    #   #         2025-01-11 08:00:00
    #   #         2025-01-21 16:00:00
    #   #         2025-02-01 00:00:00
    #   # ]
    #
    # @example When `eager: false` (default), an expression is produced. You can generate a sequence using the length of the dataframe:
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4, 5]})
    #   df.with_columns(Polars.linear_space(0, 1, Polars.len).alias("ls"))
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ ls   │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.0  │
    #   # │ 2   ┆ 0.25 │
    #   # │ 3   ┆ 0.5  │
    #   # │ 4   ┆ 0.75 │
    #   # │ 5   ┆ 1.0  │
    #   # └─────┴──────┘
    def linear_space(
      start,
      stop,
      num_samples,
      closed: "both",
      eager: false
    )
      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)
      num_samples_rbexpr = Utils.parse_into_expression(num_samples)
      result = Utils.wrap_expr(
        Plr.linear_space(start_rbexpr, end_rbexpr, num_samples_rbexpr, closed)
      )

      if eager
        return F.select(result).to_series
      end

      result
    end

    # Generate a sequence of evenly-spaced values for each row between `start` and `end`.
    #
    # The number of values in each sequence is determined by `num_samples`.
    #
    # @param start [Object]
    #   Lower bound of the range.
    # @param stop [Object]
    #   Upper bound of the range.
    # @param num_samples [Integer]
    #   Number of samples in the output sequence.
    # @param closed ['both', 'left', 'right', 'none']
    #   Define which sides of the interval are closed (inclusive).
    # @param as_array [Boolean]
    #   Return result as a fixed-length `Array`. `num_samples` must be a constant.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`.
    #   If set to `false` (default), return an expression instead.
    #
    # @return [Expr, Series]
    #
    # @note
    #   This functionality is experimental. It may be changed at any point without it
    #   being considered a breaking change.
    #
    # @example
    #   df = Polars::DataFrame.new({"start" => [1, -1], "end" => [3, 2], "num_samples" => [4, 5]})
    #   df.with_columns(ls: Polars.linear_spaces("start", "end", "num_samples"))
    #   # =>
    #   # shape: (2, 4)
    #   # ┌───────┬─────┬─────────────┬────────────────────────┐
    #   # │ start ┆ end ┆ num_samples ┆ ls                     │
    #   # │ ---   ┆ --- ┆ ---         ┆ ---                    │
    #   # │ i64   ┆ i64 ┆ i64         ┆ list[f64]              │
    #   # ╞═══════╪═════╪═════════════╪════════════════════════╡
    #   # │ 1     ┆ 3   ┆ 4           ┆ [1.0, 1.666667, … 3.0] │
    #   # │ -1    ┆ 2   ┆ 5           ┆ [-1.0, -0.25, … 2.0]   │
    #   # └───────┴─────┴─────────────┴────────────────────────┘
    #
    # @example
    #   df.with_columns(ls: Polars.linear_spaces("start", "end", 3, as_array: true))
    #   # =>
    #   # shape: (2, 4)
    #   # ┌───────┬─────┬─────────────┬──────────────────┐
    #   # │ start ┆ end ┆ num_samples ┆ ls               │
    #   # │ ---   ┆ --- ┆ ---         ┆ ---              │
    #   # │ i64   ┆ i64 ┆ i64         ┆ array[f64, 3]    │
    #   # ╞═══════╪═════╪═════════════╪══════════════════╡
    #   # │ 1     ┆ 3   ┆ 4           ┆ [1.0, 2.0, 3.0]  │
    #   # │ -1    ┆ 2   ┆ 5           ┆ [-1.0, 0.5, 2.0] │
    #   # └───────┴─────┴─────────────┴──────────────────┘
    def linear_spaces(
      start,
      stop,
      num_samples,
      closed: "both",
      as_array: false,
      eager: false
    )
      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)
      num_samples_rbexpr = Utils.parse_into_expression(num_samples)
      result = Utils.wrap_expr(
        Plr.linear_spaces(
          start_rbexpr, end_rbexpr, num_samples_rbexpr, closed, as_array
        )
      )

      if eager
        return F.select(result).to_series
      end

      result
    end
  end
end

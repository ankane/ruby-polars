module Polars
  module Functions
    # Start a "when, then, otherwise" expression.
    #
    # @return [When]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 3, 4], "bar" => [3, 4, 0]})
    #   df.with_column(Polars.when(Polars.col("foo") > 2).then(Polars.lit(1)).otherwise(Polars.lit(-1)))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────────┐
    #   # │ foo ┆ bar ┆ literal │
    #   # │ --- ┆ --- ┆ ---     │
    #   # │ i64 ┆ i64 ┆ i32     │
    #   # ╞═════╪═════╪═════════╡
    #   # │ 1   ┆ 3   ┆ -1      │
    #   # │ 3   ┆ 4   ┆ 1       │
    #   # │ 4   ┆ 0   ┆ 1       │
    #   # └─────┴─────┴─────────┘
    def when(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      pw = Plr.when(expr._rbexpr)
      When.new(pw)
    end
  end
end

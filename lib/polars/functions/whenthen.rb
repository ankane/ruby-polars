module Polars
  module Functions
    # Start a "when, then, otherwise" expression.
    #
    # @return [When]
    #
    # @example Below we add a column with the value 1, where column "foo" > 2 and the value -1 where it isn't.
    #   df = Polars::DataFrame.new({"foo" => [1, 3, 4], "bar" => [3, 4, 0]})
    #   df.with_columns(Polars.when(Polars.col("foo") > 2).then(Polars.lit(1)).otherwise(Polars.lit(-1)))
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
    #
    # @example Or with multiple when-then operations chained:
    #   df.with_columns(
    #     Polars.when(Polars.col("foo") > 2)
    #     .then(1)
    #     .when(Polars.col("bar") > 2)
    #     .then(4)
    #     .otherwise(-1)
    #     .alias("val")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ val │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i32 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 3   ┆ 4   │
    #   # │ 3   ┆ 4   ┆ 1   │
    #   # │ 4   ┆ 0   ┆ 1   │
    #   # └─────┴─────┴─────┘
    #
    # @example The `otherwise` at the end is optional. If left out, any rows where none of the `when` expressions evaluate to true, are set to `null`:
    #   df.with_columns(Polars.when(Polars.col("foo") > 2).then(1).alias("val"))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ val  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64 ┆ i32  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1   ┆ 3   ┆ null │
    #   # │ 3   ┆ 4   ┆ 1    │
    #   # │ 4   ┆ 0   ┆ 1    │
    #   # └─────┴─────┴──────┘
    #
    # @example Pass multiple predicates, each of which must be met:
    #   df.with_columns(
    #     val: Polars.when(
    #       Polars.col("bar") > 0,
    #       Polars.col("foo") % 2 != 0
    #     )
    #     .then(99)
    #     .otherwise(-1)
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ val │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i32 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 3   ┆ 99  │
    #   # │ 3   ┆ 4   ┆ 99  │
    #   # │ 4   ┆ 0   ┆ -1  │
    #   # └─────┴─────┴─────┘
    #
    # @example Pass conditions as keyword arguments:
    #   df.with_columns(val: Polars.when(foo: 4, bar: 0).then(99).otherwise(-1))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ val │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i32 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 3   ┆ -1  │
    #   # │ 3   ┆ 4   ┆ -1  │
    #   # │ 4   ┆ 0   ┆ 99  │
    #   # └─────┴─────┴─────┘
    def when(*predicates, **constraints)
      condition = Utils.parse_predicates_constraints_into_expression(*predicates, **constraints)
      When.new(Plr.when(condition))
    end
  end
end

module Polars
  module Functions
    # Return the number of rows in the context.
    #
    # This is similar to `COUNT(*)` in SQL.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil],
    #       "b" => [3, nil, nil],
    #       "c" => ["foo", "bar", "foo"]
    #     }
    #   )
    #   df.select(Polars.len)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ len │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 3   │
    #   # └─────┘
    #
    # @example Generate an index column by using `len` in conjunction with `int_range`.
    #   df.select([
    #     Polars.int_range(Polars.len, dtype: Polars::UInt32).alias("index"),
    #     Polars.all
    #   ])
    #   # =>
    #   # shape: (3, 4)
    #   # ┌───────┬──────┬──────┬─────┐
    #   # │ index ┆ a    ┆ b    ┆ c   │
    #   # │ ---   ┆ ---  ┆ ---  ┆ --- │
    #   # │ u32   ┆ i64  ┆ i64  ┆ str │
    #   # ╞═══════╪══════╪══════╪═════╡
    #   # │ 0     ┆ 1    ┆ 3    ┆ foo │
    #   # │ 1     ┆ 2    ┆ null ┆ bar │
    #   # │ 2     ┆ null ┆ null ┆ foo │
    #   # └───────┴──────┴──────┴─────┘
    def len
      Utils.wrap_expr(Plr.len)
    end
    alias_method :length, :len
  end
end

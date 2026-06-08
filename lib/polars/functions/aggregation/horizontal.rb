module Polars
  module Functions
    # Compute the bitwise AND horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [false, false, true, true, false, nil],
    #       "b" => [false, true, true, nil, nil, nil],
    #       "c" => ["u", "v", "w", "x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(all: Polars.all_horizontal("a", "b"))
    #   # =>
    #   # shape: (6, 4)
    #   # ┌───────┬───────┬─────┬───────┐
    #   # │ a     ┆ b     ┆ c   ┆ all   │
    #   # │ ---   ┆ ---   ┆ --- ┆ ---   │
    #   # │ bool  ┆ bool  ┆ str ┆ bool  │
    #   # ╞═══════╪═══════╪═════╪═══════╡
    #   # │ false ┆ false ┆ u   ┆ false │
    #   # │ false ┆ true  ┆ v   ┆ false │
    #   # │ true  ┆ true  ┆ w   ┆ true  │
    #   # │ true  ┆ null  ┆ x   ┆ null  │
    #   # │ false ┆ null  ┆ y   ┆ false │
    #   # │ null  ┆ null  ┆ z   ┆ null  │
    #   # └───────┴───────┴─────┴───────┘
    def all_horizontal(*exprs)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      Utils.wrap_expr(Plr.all_horizontal(rbexprs))
    end

    # Compute the bitwise OR horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [false, false, true, true, false, nil],
    #       "b" => [false, true, true, nil, nil, nil],
    #       "c" => ["u", "v", "w", "x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(any: Polars.any_horizontal("a", "b"))
    #   # =>
    #   # shape: (6, 4)
    #   # ┌───────┬───────┬─────┬───────┐
    #   # │ a     ┆ b     ┆ c   ┆ any   │
    #   # │ ---   ┆ ---   ┆ --- ┆ ---   │
    #   # │ bool  ┆ bool  ┆ str ┆ bool  │
    #   # ╞═══════╪═══════╪═════╪═══════╡
    #   # │ false ┆ false ┆ u   ┆ false │
    #   # │ false ┆ true  ┆ v   ┆ true  │
    #   # │ true  ┆ true  ┆ w   ┆ true  │
    #   # │ true  ┆ null  ┆ x   ┆ true  │
    #   # │ false ┆ null  ┆ y   ┆ null  │
    #   # │ null  ┆ null  ┆ z   ┆ null  │
    #   # └───────┴───────┴─────┴───────┘
    def any_horizontal(*exprs)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      Utils.wrap_expr(Plr.any_horizontal(rbexprs))
    end

    # Get the maximum value horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(max: Polars.max_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬─────┐
    #   # │ a   ┆ b    ┆ c   ┆ max │
    #   # │ --- ┆ ---  ┆ --- ┆ --- │
    #   # │ i64 ┆ i64  ┆ str ┆ i64 │
    #   # ╞═════╪══════╪═════╪═════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 4   │
    #   # │ 8   ┆ 5    ┆ y   ┆ 8   │
    #   # │ 3   ┆ null ┆ z   ┆ 3   │
    #   # └─────┴──────┴─────┴─────┘
    def max_horizontal(*exprs)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      Utils.wrap_expr(Plr.max_horizontal(rbexprs))
    end

    # Get the minimum value horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(min: Polars.min_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬─────┐
    #   # │ a   ┆ b    ┆ c   ┆ min │
    #   # │ --- ┆ ---  ┆ --- ┆ --- │
    #   # │ i64 ┆ i64  ┆ str ┆ i64 │
    #   # ╞═════╪══════╪═════╪═════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 1   │
    #   # │ 8   ┆ 5    ┆ y   ┆ 5   │
    #   # │ 3   ┆ null ┆ z   ┆ 3   │
    #   # └─────┴──────┴─────┴─────┘
    def min_horizontal(*exprs)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      Utils.wrap_expr(Plr.min_horizontal(rbexprs))
    end

    # Sum all values horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #   If set to `false`, any null value in the input will lead to a null output.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(sum: Polars.sum_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬─────┐
    #   # │ a   ┆ b    ┆ c   ┆ sum │
    #   # │ --- ┆ ---  ┆ --- ┆ --- │
    #   # │ i64 ┆ i64  ┆ str ┆ i64 │
    #   # ╞═════╪══════╪═════╪═════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 5   │
    #   # │ 8   ┆ 5    ┆ y   ┆ 13  │
    #   # │ 3   ┆ null ┆ z   ┆ 3   │
    #   # └─────┴──────┴─────┴─────┘
    def sum_horizontal(*exprs, ignore_nulls: true)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      Utils.wrap_expr(Plr.sum_horizontal(rbexprs, ignore_nulls))
    end

    # Compute the mean of all values horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #   If set to `false`, any null value in the input will lead to a null output.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(mean: Polars.mean_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬──────┐
    #   # │ a   ┆ b    ┆ c   ┆ mean │
    #   # │ --- ┆ ---  ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64  ┆ str ┆ f64  │
    #   # ╞═════╪══════╪═════╪══════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ 2.5  │
    #   # │ 8   ┆ 5    ┆ y   ┆ 6.5  │
    #   # │ 3   ┆ null ┆ z   ┆ 3.0  │
    #   # └─────┴──────┴─────┴──────┘
    def mean_horizontal(*exprs, ignore_nulls: true)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      Utils.wrap_expr(Plr.mean_horizontal(rbexprs, ignore_nulls))
    end

    # Cumulatively sum all values horizontally across columns.
    #
    # @param exprs [Array]
    #   Column(s) to use in the aggregation. Accepts expression input. Strings are
    #   parsed as column names, other non-expression inputs are parsed as literals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 8, 3],
    #       "b" => [4, 5, nil],
    #       "c" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.cum_sum_horizontal("a", "b"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬─────┬───────────┐
    #   # │ a   ┆ b    ┆ c   ┆ cum_sum   │
    #   # │ --- ┆ ---  ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64  ┆ str ┆ struct[2] │
    #   # ╞═════╪══════╪═════╪═══════════╡
    #   # │ 1   ┆ 4    ┆ x   ┆ {1,5}     │
    #   # │ 8   ┆ 5    ┆ y   ┆ {8,13}    │
    #   # │ 3   ┆ null ┆ z   ┆ {3,null}  │
    #   # └─────┴──────┴─────┴───────────┘
    def cum_sum_horizontal(*exprs)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs)
      exprs_wrapped = rbexprs.map { |e| Utils.wrap_expr(e) }

      Polars.cum_fold(
        Polars.lit(0).cast(Polars.dtype_of(Polars.sum_horizontal(exprs))),
        exprs_wrapped
      ) { |a, b| a + b }.alias("cum_sum")
    end
  end
end

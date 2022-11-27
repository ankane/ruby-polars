module Polars
  # Namespace for categorical related expressions.
  class CatExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Determine how this categorical series should be sorted.
    #
    # @param ordering ["physical", "lexical"]
    #   Ordering type:
    #
    #   - 'physical' -> Use the physical representation of the categories to determine the order (default).
    #   - 'lexical' -> Use the string values to determine the ordering.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"cats" => ["z", "z", "k", "a", "b"], "vals" => [3, 1, 2, 2, 3]}
    #   ).with_columns(
    #     [
    #       Polars.col("cats").cast(:cat).cat.set_ordering("lexical")
    #     ]
    #   )
    #   df.sort(["cats", "vals"])
    #   # =>
    #   # shape: (5, 2)
    #   # ┌──────┬──────┐
    #   # │ cats ┆ vals │
    #   # │ ---  ┆ ---  │
    #   # │ cat  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ a    ┆ 2    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ b    ┆ 3    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ k    ┆ 2    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ z    ┆ 1    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ z    ┆ 3    │
    #   # └──────┴──────┘
    def set_ordering(ordering)
      Utils.wrap_expr(_rbexpr.cat_set_ordering(ordering))
    end
  end
end

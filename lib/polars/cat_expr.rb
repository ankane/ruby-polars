module Polars
  # Namespace for categorical related expressions.
  class CatExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Get the categories stored in this data type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::Series.new(
    #     "cats", ["foo", "bar", "foo", "foo", "ham"], dtype: Polars::Categorical
    #   ).to_frame
    #   df.select(Polars.col("cats").cat.get_categories)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ cats │
    #   # │ ---  │
    #   # │ str  │
    #   # ╞══════╡
    #   # │ foo  │
    #   # │ bar  │
    #   # │ ham  │
    #   # └──────┘
    def get_categories
      Utils.wrap_expr(_rbexpr.cat_get_categories)
    end
  end
end

module Polars
  class CatExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    def set_ordering(ordering)
      Utils.wrap_expr(_rbexpr.cat_set_ordering(ordering))
    end
  end
end

module Polars
  class WhenThen
    attr_accessor :_rbwhenthen

    def initialize(rbwhenthen)
      self._rbwhenthen = rbwhenthen
    end

    def when(predicate)
      WhenThenThen.new(_rbwhenthen.when(predicate._rbexpr))
    end

    def otherwise(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      Utils.wrap_expr(_rbwhenthen.otherwise(expr._rbexpr))
    end
  end
end

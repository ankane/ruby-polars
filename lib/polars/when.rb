module Polars
  class When
    attr_accessor :_rbwhen

    def initialize(rbwhen)
      self._rbwhen = rbwhen
    end

    def then(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      rbwhenthen = _rbwhen._then(expr._rbexpr)
      WhenThen.new(rbwhenthen)
    end
  end
end

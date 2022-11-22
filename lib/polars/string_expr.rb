module Polars
  class StringExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    def lengths
      Utils.wrap_expr(_rbexpr.str_lengths)
    end

    def contains(pattern, literal: false)
      Utils.wrap_expr(_rbexpr.str_contains(pattern, literal))
    end
  end
end

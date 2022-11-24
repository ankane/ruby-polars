module Polars
  class StructExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # def [](item)
    # end

    # def field
    # end

    # def rename_fields
    # end
  end
end

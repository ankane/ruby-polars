module Polars
  # Namespace for extension type related expressions.
  class ExtExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end
  end
end

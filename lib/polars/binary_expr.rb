module Polars
  # Namespace for binary related expressions.
  class BinaryExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end
  end
end

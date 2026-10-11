module Polars
  # Namespace for map related expressions.
  class MapExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end
  end
end

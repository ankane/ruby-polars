module Polars
  # Namespace for expressions on a meta level.
  class MetaExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Equal.
    #
    # @return [Boolean]
    def ==(other)
      _rbexpr.meta_eq(other._rbexpr)
    end

    # Not equal.
    #
    # @return [Boolean]
    def !=(other)
      !(self == other)
    end

    # Pop the latest expression and return the input(s) of the popped expression.
    #
    # @return [Array]
    def pop
      _rbexpr.meta_pop.map { |e| Utils.wrap_expr(e) }
    end

    # Get a list with the root column name.
    #
    # @return [Array]
    def root_names
      _rbexpr.meta_roots
    end

    # Get the column name that this expression would produce.
    #
    # @return [String]
    def output_name
      _rbexpr.meta_output_name
    end

    # Undo any renaming operation like `alias` or `keep_name`.
    #
    # @return [Expr]
    def undo_aliases
      Utils.wrap_expr(_rbexpr.meta_undo_aliases)
    end
  end
end

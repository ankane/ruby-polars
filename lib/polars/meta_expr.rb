module Polars
  class MetaExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    def ==(other)
      _rbexpr.meta_eq(other._rbexpr)
    end

    def !=(other)
      !(self == other)
    end

    def pop
      _rbexpr.meta_pop.map { |e| Utils.wrap_expr(e) }
    end

    def root_names
      _rbexpr.meta_roots
    end

    def output_name
      _rbexpr.meta_output_name
    end

    def undo_aliases
      Utils.wrap_expr(_rbexpr.meta_undo_aliases)
    end
  end
end

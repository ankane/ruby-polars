module Polars
  class MetaExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # def ==(other)
    # end

    # def !=(other)
    # end

    # def pop
    # end

    # def root_names
    # end

    # def output_name
    # end

    # def undo_aliases
    # end
  end
end

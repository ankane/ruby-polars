module Polars
  class StructExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    def [](item)
      if item.is_a?(String)
        field(item)
      elsif item.is_a?(Integer)
        Utils.wrap_expr(_rbexpr.struct_field_by_index(item))
      else
        raise ArgumentError, "expected type Integer or String, got #{item.class.name}"
      end
    end

    def field(name)
      Utils.wrap_expr(_rbexpr.struct_field_by_name(name))
    end

    def rename_fields(names)
      Utils.wrap_expr(_rbexpr.struct_rename_fields(names))
    end
  end
end

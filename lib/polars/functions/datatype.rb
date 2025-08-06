module Polars
  module Functions
    # Get a lazily evaluated :class:`DataType` of a column or expression.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @return [DataTypeExpr]
    def dtype_of(col_or_expr)
      e = nil
      if col_or_expr.is_a?(::String)
        e = F.col(col_or_expr)
      else
        e = col_or_expr
      end

      DataTypeExpr._from_rbdatatype_expr(RbDataTypeExpr.of_expr(e._rbexpr))
    end
  end
end

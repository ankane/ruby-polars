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

    # Get the dtype of `self` in `map_elements` and `map_batches`.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @return [DataTypeExpr]
    def self_dtype
      DataTypeExpr._from_rbdatatype_expr(RbDataTypeExpr.self_dtype)
    end

    # Create a new datatype expression that represents a Struct datatype.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @return [DataTypeExpr]
    def struct_with_fields(
      mapping
    )
      preprocess = lambda do |dtype_expr|
        if dtype_expr.is_a?(DataType)
          dtype_expr.to_dtype_expr._rbdatatype_expr
        elsif dtype_expr < DataType
          dtype_expr.to_dtype_expr._rbdatatype_expr
        elsif dtype_expr.is_a?(DataTypeExpr)
          dtype_expr._rbdatatype_expr
        else
          msg = "mapping item must be a datatype or datatype expression; found #{dtype_expr.inspect}"
          raise TypeError, msg
        end
      end

      fields = mapping.map { |name, dtype_expr| [name.to_s, preprocess.(dtype_expr)]  }

      DataTypeExpr._from_rbdatatype_expr(
        RbDataTypeExpr.struct_with_fields(fields)
      )
    end
  end
end

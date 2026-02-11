module Polars
  # Namespace for extension type related expressions.
  class ExtensionExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Convert to an extension `dtype`.
    #
    # The input must be of the storage type of the extension dtype.
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    #
    # @return [Expr]
    def to(dtype)
      rb_dtype = Utils.parse_into_datatype_expr(dtype)._rbdatatype_expr
      Utils.wrap_expr(_rbexpr.ext_to(rb_dtype))
    end

    # Get the storage values of an extension data type.
    #
    # If the input does not have an extension data type, it is returned as-is.
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    #
    # @return [Expr]
    def storage
      Utils.wrap_expr(_rbexpr.ext_storage)
    end
  end
end

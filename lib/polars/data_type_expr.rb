module Polars
  # A lazily instantiated `DataType` that can be used in an `Expr`.
  class DataTypeExpr
    # @private
    attr_accessor :_rbdatatype_expr

    # @private
    def self._from_rbdatatype_expr(rbdatatype_expr)
      slf = new
      slf._rbdatatype_expr = rbdatatype_expr
      slf
    end

    # Materialize the `DataTypeExpr` in a specific context.
    #
    # This is a useful function when debugging datatype expressions.
    #
    # @return [DataType]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 2, 3]
    #     }
    #   )
    #   Polars.dtype_of("a").collect_dtype(lf)
    #   # => Polars::Int64
    #
    # @example
    #   Polars.dtype_of("a").collect_dtype({"a" => Polars::String})
    #   # => Polars::String
    def collect_dtype(
      context
    )
      schema = nil
      if context.is_a?(Schema)
         schema = context
      elsif context.is_a?(Hash)
        schema = Schema.new(context)
      elsif context.is_a?(DataFrame)
        schema = context.schema
      elsif context.is_a?(LazyFrame)
        schema = context.collect_schema
      else
        msg = "DataTypeExpr.collect_dtype did not expect #{context.inspect}"
        raise TypeError, msg
      end

      _rbdatatype_expr.collect_dtype(schema.to_h)
    end
  end
end

module Polars
  module Functions
    # Repeat a single value n times.
    #
    # @param value [Object]
    #   Value to repeat.
    # @param n [Integer]
    #   Repeat `n` times.
    # @param dtype [Object]
    #   Data type of the resulting column. If set to `nil` (default), data type is
    #   inferred from the given value. Defaults to Int32 for integer values, unless
    #   Int64 is required to fit the given value. Defaults to Float64 for float values.
    # @param eager [Boolean]
    #   Run eagerly and collect into a `Series`.
    # @param name [String]
    #   Only used in `eager` mode. As expression, use `alias`.
    #
    # @return [Object]
    #
    # @example Construct a column with a repeated value in a lazy context.
    #   Polars.select(Polars.repeat("z", 3)).to_series
    #   # =>
    #   # shape: (3,)
    #   # Series: 'repeat' [str]
    #   # [
    #   #         "z"
    #   #         "z"
    #   #         "z"
    #   # ]
    #
    # @example Generate a Series directly by setting `eager: true`.
    #   Polars.repeat(3, 3, dtype: Polars::Int8, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'repeat' [i8]
    #   # [
    #   #         3
    #   #         3
    #   #         3
    #   # ]
    def repeat(value, n, dtype: nil, eager: false, name: nil)
      if !name.nil?
        warn "the `name` argument is deprecated. Use the `alias` method instead."
      end

      if n.is_a?(Integer)
        n = lit(n)
      end

      value = Utils.parse_into_expression(value, str_as_lit: true)
      expr = Utils.wrap_expr(Plr.repeat(value, n._rbexpr, dtype))
      if !name.nil?
        expr = expr.alias(name)
      end
      if eager
        return select(expr).to_series
      end
      expr
    end

    # Construct a column of length `n` filled with ones.
    #
    # This is syntactic sugar for the `repeat` function.
    #
    # @param n [Integer]
    #   Length of the resulting column.
    # @param dtype [Object]
    #   Data type of the resulting column. Defaults to Float64.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`. If set to `false`,
    #   return an expression instead.
    #
    # @return [Object]
    #
    # @example
    #   Polars.ones(3, dtype: Polars::Int8, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'ones' [i8]
    #   # [
    #   #         1
    #   #         1
    #   #         1
    #   # ]
    def ones(n, dtype: nil, eager: true)
      if (zero = _one_or_zero_by_dtype(1, dtype)).nil?
        msg = "invalid dtype for `ones`; found #{dtype}"
        raise TypeError, msg
      end

      repeat(zero, n, dtype: dtype, eager: eager).alias("ones")
    end

    # Construct a column of length `n` filled with zeros.
    #
    # This is syntactic sugar for the `repeat` function.
    #
    # @param n [Integer]
    #   Length of the resulting column.
    # @param dtype [Object]
    #   Data type of the resulting column. Defaults to Float64.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`. If set to `false`,
    #   return an expression instead.
    #
    # @return [Object]
    #
    # @example
    #   Polars.zeros(3, dtype: Polars::Int8, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'zeros' [i8]
    #   # [
    #   #         0
    #   #         0
    #   #         0
    #   # ]
    def zeros(n, dtype: nil, eager: true)
      if (zero = _one_or_zero_by_dtype(0, dtype)).nil?
        msg = "invalid dtype for `zeros`; found #{dtype}"
        raise TypeError, msg
      end

      repeat(zero, n, dtype: dtype, eager: eager).alias("zeros")
    end

    private

    def _one_or_zero_by_dtype(value, dtype)
      if dtype.integer?
        value
      elsif dtype.float?
        value.to_f
      elsif dtype == Boolean
        value != 0
      elsif dtype == Utf8
        value.to_s
      elsif dtype == Decimal
        Decimal(value.to_s)
      elsif [List, Array].include?(dtype)
        arr_width = dtype.respond_to?(:width) ? dtype.width : 1
        [_one_or_zero_by_dtype(value, dtype.inner)] * arr_width
      else
        nil
      end
    end
  end
end

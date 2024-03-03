module Polars
  module Functions
    # Repeat a single value n times.
    #
    # @param value [Object]
    #   Value to repeat.
    # @param n [Integer]
    #   Repeat `n` times.
    # @param eager [Boolean]
    #   Run eagerly and collect into a `Series`.
    # @param name [String]
    #   Only used in `eager` mode. As expression, use `alias`.
    #
    # @return [Expr]
    def repeat(value, n, dtype: nil, eager: false, name: nil)
      if !name.nil?
        warn "the `name` argument is deprecated. Use the `alias` method instead."
      end

      if n.is_a?(Integer)
        n = lit(n)
      end

      value = Utils.parse_as_expression(value, str_as_lit: true)
      expr = Utils.wrap_expr(Plr.repeat(value, n._rbexpr, dtype))
      if !name.nil?
        expr = expr.alias(name)
      end
      if eager
        return select(expr).to_series
      end
      expr
    end

    # Return a new Series of given length and type, filled with ones.
    #
    # @param n [Integer]
    #   Number of elements in the `Series`
    # @param dtype [Symbol]
    #   DataType of the elements, defaults to `:f64`
    #
    # @return [Series]
    #
    # @note
    #   In the lazy API you should probably not use this, but use `lit(1)`
    #   instead.
    def ones(n, dtype: nil)
      s = Series.new([1.0])
      if dtype
        s = s.cast(dtype)
      end
      s.new_from_index(0, n)
    end

    # Return a new Series of given length and type, filled with zeros.
    #
    # @param n [Integer]
    #   Number of elements in the `Series`
    # @param dtype [Symbol]
    #   DataType of the elements, defaults to `:f64`
    #
    # @return [Series]
    #
    # @note
    #   In the lazy API you should probably not use this, but use `lit(0)`
    #   instead.
    def zeros(n, dtype: nil)
      s = Series.new([0.0])
      if dtype
        s = s.cast(dtype)
      end
      s.new_from_index(0, n)
    end
  end
end

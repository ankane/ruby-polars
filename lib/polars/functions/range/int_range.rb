module Polars
  module Functions
    # Create a range expression (or Series).
    #
    # This can be used in a `select`, `with_column`, etc. Be sure that the resulting
    # range size is equal to the length of the DataFrame you are collecting.
    #
    # @param start [Integer, Expr, Series]
    #   Lower bound of range.
    # @param stop [Integer, Expr, Series]
    #   Upper bound of range.
    # @param step [Integer]
    #   Step size of the range.
    # @param eager [Boolean]
    #   If eager evaluation is `True`, a Series is returned instead of an Expr.
    # @param dtype [Symbol]
    #   Apply an explicit integer dtype to the resulting expression (default is `Int64`).
    #
    # @return [Expr, Series]
    #
    # @example
    #   Polars.arange(0, 3, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'arange' [i64]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   # ]
    def int_range(start, stop = nil, step: 1, eager: false, dtype: nil)
      if stop.nil?
        stop = start
        start = 0
      end

      start = Utils.parse_into_expression(start)
      stop = Utils.parse_into_expression(stop)
      dtype ||= Int64
      dtype = dtype.to_s if dtype.is_a?(Symbol)
      result = Utils.wrap_expr(Plr.int_range(start, stop, step, dtype)).alias("arange")

      if eager
        return select(result).to_series
      end

      result
    end
    alias_method :arange, :int_range
  end
end

module Polars
  # @private
  module ExprDispatch
    private

    def method_missing(method, ...)
      return super unless self.class.method_defined?(method)

      s = Utils.wrap_s(_s)
      expr = Utils.col(s.name)
      s.to_frame.select(expr.send(method, ...)).to_series
    end
  end
end

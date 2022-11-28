module Polars
  # @private
  module ExprDispatch
    private

    def self.included(base)
      base.attr_accessor :_s
      base.singleton_class.attr_accessor :_accessor
    end

    def method_missing(method, ...)
      return super unless self.class.method_defined?(method)

      namespace = self.class._accessor

      s = Utils.wrap_s(_s)
      expr = Utils.col(s.name)
      expr = expr.send(namespace) if namespace
      s.to_frame.select(expr.send(method, ...)).to_series
    end
  end
end

module Polars
  # Series.str namespace.
  class StringNameSpace
    include ExprDispatch

    # @private
    def self._accessor
      "str"
    end

    # @private
    attr_accessor :_s

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

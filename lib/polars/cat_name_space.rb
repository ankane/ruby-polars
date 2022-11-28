module Polars
  # Series.cat namespace.
  class CatNameSpace
    include ExprDispatch

    # @private
    def self._accessor
      "cat"
    end

    # @private
    attr_accessor :_s

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

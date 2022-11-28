module Polars
  # Series.struct namespace.
  class StructNameSpace
    include ExprDispatch

    # @private
    def self._accessor
      "struct"
    end

    # @private
    attr_accessor :_s

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

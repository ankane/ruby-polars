module Polars
  # Series.dt namespace.
  class DateTimeNameSpace
    include ExprDispatch

    # @private
    def self._accessor
      "dt"
    end

    # @private
    attr_accessor :_s

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

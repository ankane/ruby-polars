module Polars
  # Series.cat namespace.
  class CatNameSpace
    include ExprDispatch

    self._accessor = "cat"

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

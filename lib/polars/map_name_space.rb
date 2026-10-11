module Polars
  # Series.map namespace.
  class MapNameSpace
    include ExprDispatch

    self._accessor = "map"

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

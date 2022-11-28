module Polars
  # Series.struct namespace.
  class StructNameSpace
    include ExprDispatch

    self._accessor = "struct"

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

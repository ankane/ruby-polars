module Polars
  # Series.ext namespace.
  class ExtensionNameSpace
    include ExprDispatch

    self._accessor = "ext"

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

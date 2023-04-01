module Polars
  # Series.bin namespace.
  class BinaryNameSpace
    include ExprDispatch

    self._accessor = "bin"

    # @private
    def initialize(series)
      self._s = series._s
    end
  end
end

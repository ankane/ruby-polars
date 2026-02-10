module Polars
  # Series.ext namespace.
  class ExtensionNameSpace
    include ExprDispatch

    self._accessor = "ext"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Create a Series with an extension `dtype`.
    #
    # The input series must have the storage type of the extension dtype.
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    #
    # @return [Series]
    def to(dtype)
      Utils.wrap_s(_s.ext_to(dtype))
    end

    # Get the storage values of a Series with an extension data type.
    #
    # If the input series does not have an extension data type, it is returned as-is.
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    #
    # @return [Series]
    def storage
      Utils.wrap_s(_s.ext_storage)
    end
  end
end

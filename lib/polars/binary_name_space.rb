module Polars
  # Series.bin namespace.
  class BinaryNameSpace
    include ExprDispatch

    self._accessor = "bin"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Check if binaries in Series contain a binary substring.
    #
    # @param lit [String]
    #   The binary substring to look for
    #
    # @return [Series]
    def contains(lit)
      super
    end

    # Check if string values end with a binary substring.
    #
    # @param sub [String]
    #   Suffix substring.
    #
    # @return [Series]
    def ends_with(sub)
      super
    end

    # Check if values start with a binary substring.
    #
    # @param sub [String]
    #   Prefix substring.
    #
    # @return [Series]
    def starts_with(sub)
      super
    end

    # Decode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    # @param strict [Boolean]
    #   Raise an error if the underlying value cannot be decoded,
    #   otherwise mask out with a null value.
    #
    # @return [Series]
    def decode(encoding, strict: true)
      super
    end

    # Encode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    #
    # @return [Series]
    def encode(encoding)
      super
    end
  end
end

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
    # @param literal [String]
    #   The binary substring to look for
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("colors", ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b])
    #   s.bin.contains("\xff".b)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   # ]
    def contains(literal)
      super
    end

    # Check if string values end with a binary substring.
    #
    # @param suffix [String]
    #   Suffix substring.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("colors", ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b])
    #   s.bin.ends_with("\x00".b)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def ends_with(suffix)
      super
    end

    # Check if values start with a binary substring.
    #
    # @param prefix [String]
    #   Prefix substring.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("colors", ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b])
    #   s.bin.starts_with("\x00".b)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         true
    #   # ]
    def starts_with(prefix)
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
    #
    # @example Decode values using hexadecimal encoding.
    #   s = Polars::Series.new("colors", ["000000".b, "ffff00".b, "0000ff".b])
    #   s.bin.decode("hex")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [binary]
    #   # [
    #   #         b"\x00\x00\x00"
    #   #         b"\xff\xff\x00"
    #   #         b"\x00\x00\xff"
    #   # ]
    #
    # @example Decode values using Base64 encoding.
    #   s = Polars::Series.new("colors", ["AAAA".b, "//8A".b, "AAD/".b])
    #   s.bin.decode("base64")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [binary]
    #   # [
    #   #         b"\x00\x00\x00"
    #   #         b"\xff\xff\x00"
    #   #         b"\x00\x00\xff"
    #   # ]
    #
    # @example Set `strict=False` to set invalid values to null instead of raising an error.
    #   s = Polars::Series.new("colors", ["000000".b, "ffff00".b, "invalid_value".b])
    #   s.bin.decode("hex", strict: false)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [binary]
    #   # [
    #   #         b"\x00\x00\x00"
    #   #         b"\xff\xff\x00"
    #   #         null
    #   # ]
    def decode(encoding, strict: true)
      super
    end

    # Encode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    #
    # @return [Series]
    #
    # @example Encode values using hexadecimal encoding.
    #   s = Polars::Series.new("colors", ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b])
    #   s.bin.encode("hex")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [str]
    #   # [
    #   #         "000000"
    #   #         "ffff00"
    #   #         "0000ff"
    #   # ]
    #
    # @example Encode values using Base64 encoding.
    #   s.bin.encode("base64")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'colors' [str]
    #   # [
    #   #         "AAAA"
    #   #         "//8A"
    #   #         "AAD/"
    #   # ]
    def encode(encoding)
      super
    end
  end
end

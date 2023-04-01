module Polars
  # Namespace for binary related expressions.
  class BinaryExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Check if binaries in Series contain a binary substring.
    #
    # @param lit [String]
    #   The binary substring to look for
    #
    # @return [Expr]
    def contains(lit)
      Utils.wrap_expr(_rbexpr.binary_contains(lit))
    end

    # Check if string values end with a binary substring.
    #
    # @param sub [String]
    #   Suffix substring.
    #
    # @return [Expr]
    def ends_with(sub)
      Utils.wrap_expr(_rbexpr.binary_ends_with(sub))
    end

    # Check if values start with a binary substring.
    #
    # @param sub [String]
    #   Prefix substring.
    #
    # @return [Expr]
    def starts_with(sub)
      Utils.wrap_expr(_rbexpr.binary_starts_with(sub))
    end

    # Decode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    # @param strict [Boolean]
    #   Raise an error if the underlying value cannot be decoded,
    #   otherwise mask out with a null value.
    #
    # @return [Expr]
    def decode(encoding, strict: true)
      if encoding == "hex"
        Utils.wrap_expr(_rbexpr.binary_hex_decode(strict))
      elsif encoding == "base64"
        Utils.wrap_expr(_rbexpr.binary_base64_decode(strict))
      else
        raise ArgumentError, "encoding must be one of {{'hex', 'base64'}}, got #{encoding}"
      end
    end

    # Encode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    #
    # @return [Expr]
    def encode(encoding)
      if encoding == "hex"
        Utils.wrap_expr(_rbexpr.binary_hex_encode)
      elsif encoding == "base64"
        Utils.wrap_expr(_rbexpr.binary_base64_encode)
      else
        raise ArgumentError, "encoding must be one of {{'hex', 'base64'}}, got #{encoding}"
      end
    end
  end
end

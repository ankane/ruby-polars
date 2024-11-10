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
    # @param literal [String]
    #   The binary substring to look for
    #
    # @return [Expr]
    #
    # @example
    #   colors = Polars::DataFrame.new(
    #     {
    #       "name" => ["black", "yellow", "blue"],
    #       "code" => ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b],
    #       "lit" => ["\x00".b, "\xff\x00".b, "\xff\xff".b]
    #     }
    #   )
    #   colors.select(
    #     "name",
    #     Polars.col("code").bin.contains("\xff".b).alias("contains_with_lit"),
    #     Polars.col("code").bin.contains(Polars.col("lit")).alias("contains_with_expr"),
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬───────────────────┬────────────────────┐
    #   # │ name   ┆ contains_with_lit ┆ contains_with_expr │
    #   # │ ---    ┆ ---               ┆ ---                │
    #   # │ str    ┆ bool              ┆ bool               │
    #   # ╞════════╪═══════════════════╪════════════════════╡
    #   # │ black  ┆ false             ┆ true               │
    #   # │ yellow ┆ true              ┆ true               │
    #   # │ blue   ┆ true              ┆ false              │
    #   # └────────┴───────────────────┴────────────────────┘
    def contains(literal)
      literal = Utils.parse_into_expression(literal, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.binary_contains(literal))
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

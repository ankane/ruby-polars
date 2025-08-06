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
    # @param suffix [String]
    #   Suffix substring.
    #
    # @return [Expr]
    #
    # @example
    #   colors = Polars::DataFrame.new(
    #     {
    #       "name" => ["black", "yellow", "blue"],
    #       "code" => ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b],
    #       "suffix" => ["\x00".b, "\xff\x00".b, "\x00\x00".b]
    #     }
    #   )
    #   colors.select(
    #     "name",
    #     Polars.col("code").bin.ends_with("\xff".b).alias("ends_with_lit"),
    #     Polars.col("code").bin.ends_with(Polars.col("suffix")).alias("ends_with_expr")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬───────────────┬────────────────┐
    #   # │ name   ┆ ends_with_lit ┆ ends_with_expr │
    #   # │ ---    ┆ ---           ┆ ---            │
    #   # │ str    ┆ bool          ┆ bool           │
    #   # ╞════════╪═══════════════╪════════════════╡
    #   # │ black  ┆ false         ┆ true           │
    #   # │ yellow ┆ false         ┆ true           │
    #   # │ blue   ┆ true          ┆ false          │
    #   # └────────┴───────────────┴────────────────┘
    def ends_with(suffix)
      suffix = Utils.parse_into_expression(suffix, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.binary_ends_with(suffix))
    end

    # Check if values start with a binary substring.
    #
    # @param prefix [String]
    #   Prefix substring.
    #
    # @return [Expr]
    #
    # @example
    #   colors = Polars::DataFrame.new(
    #     {
    #       "name": ["black", "yellow", "blue"],
    #       "code": ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b],
    #       "prefix": ["\x00".b, "\xff\x00".b, "\x00\x00".b]
    #     }
    #   )
    #   colors.select(
    #     "name",
    #     Polars.col("code").bin.starts_with("\xff".b).alias("starts_with_lit"),
    #     Polars.col("code")
    #     .bin.starts_with(Polars.col("prefix"))
    #     .alias("starts_with_expr")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────────────────┬──────────────────┐
    #   # │ name   ┆ starts_with_lit ┆ starts_with_expr │
    #   # │ ---    ┆ ---             ┆ ---              │
    #   # │ str    ┆ bool            ┆ bool             │
    #   # ╞════════╪═════════════════╪══════════════════╡
    #   # │ black  ┆ false           ┆ true             │
    #   # │ yellow ┆ true            ┆ false            │
    #   # │ blue   ┆ false           ┆ true             │
    #   # └────────┴─────────────────┴──────────────────┘
    def starts_with(prefix)
      prefix = Utils.parse_into_expression(prefix, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.binary_starts_with(prefix))
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
    #
    # @example
    #   colors = Polars::DataFrame.new(
    #     {
    #       "name" => ["black", "yellow", "blue"],
    #       "encoded" => ["000000".b, "ffff00".b, "0000ff".b]
    #     }
    #   )
    #   colors.with_columns(
    #     Polars.col("encoded").bin.decode("hex").alias("code")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬───────────┬─────────────────┐
    #   # │ name   ┆ encoded   ┆ code            │
    #   # │ ---    ┆ ---       ┆ ---             │
    #   # │ str    ┆ binary    ┆ binary          │
    #   # ╞════════╪═══════════╪═════════════════╡
    #   # │ black  ┆ b"000000" ┆ b"\x00\x00\x00" │
    #   # │ yellow ┆ b"ffff00" ┆ b"\xff\xff\x00" │
    #   # │ blue   ┆ b"0000ff" ┆ b"\x00\x00\xff" │
    #   # └────────┴───────────┴─────────────────┘
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
    #
    # @example
    #   colors = Polars::DataFrame.new(
    #     {
    #       "color" => ["black", "yellow", "blue"],
    #       "code" => ["\x00\x00\x00".b, "\xff\xff\x00".b, "\x00\x00\xff".b]
    #     }
    #   )
    #   colors.with_columns(
    #     Polars.col("code").bin.encode("hex").alias("encoded")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────────────────┬─────────┐
    #   # │ color  ┆ code            ┆ encoded │
    #   # │ ---    ┆ ---             ┆ ---     │
    #   # │ str    ┆ binary          ┆ str     │
    #   # ╞════════╪═════════════════╪═════════╡
    #   # │ black  ┆ b"\x00\x00\x00" ┆ 000000  │
    #   # │ yellow ┆ b"\xff\xff\x00" ┆ ffff00  │
    #   # │ blue   ┆ b"\x00\x00\xff" ┆ 0000ff  │
    #   # └────────┴─────────────────┴─────────┘
    def encode(encoding)
      if encoding == "hex"
        Utils.wrap_expr(_rbexpr.binary_hex_encode)
      elsif encoding == "base64"
        Utils.wrap_expr(_rbexpr.binary_base64_encode)
      else
        raise ArgumentError, "encoding must be one of {{'hex', 'base64'}}, got #{encoding}"
      end
    end

    # Get the size of binary values in the given unit.
    #
    # @param unit ['b', 'kb', 'mb', 'gb', 'tb']
    #   Scale the returned size to the given unit.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"data" => [512, 256, 1024].map { |n| "\x00".b * n }})
    #   df.with_columns(
    #     n_bytes: Polars.col("data").bin.size,
    #     n_kilobytes: Polars.col("data").bin.size("kb")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────────────────────────────┬─────────┬─────────────┐
    #   # │ data                            ┆ n_bytes ┆ n_kilobytes │
    #   # │ ---                             ┆ ---     ┆ ---         │
    #   # │ binary                          ┆ u32     ┆ f64         │
    #   # ╞═════════════════════════════════╪═════════╪═════════════╡
    #   # │ b"\x00\x00\x00\x00\x00\x00\x00… ┆ 512     ┆ 0.5         │
    #   # │ b"\x00\x00\x00\x00\x00\x00\x00… ┆ 256     ┆ 0.25        │
    #   # │ b"\x00\x00\x00\x00\x00\x00\x00… ┆ 1024    ┆ 1.0         │
    #   # └─────────────────────────────────┴─────────┴─────────────┘
    def size(unit = "b")
      sz = Utils.wrap_expr(_rbexpr.bin_size_bytes)
      sz = Utils.scale_bytes(sz, to: unit)
      sz
    end

    # Interpret a buffer as a numerical Polars type.
    #
    # @param dtype [Object]
    #   Which type to interpret binary column into.
    # @param endianness : ["big", "little"]
    #   Which endianness to use when interpreting bytes, by default "little".
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"data" => ["\x05\x00\x00\x00".b, "\x10\x00\x01\x00".b]})
    #   df.with_columns(
    #     bin2int: Polars.col("data").bin.reinterpret(
    #      dtype: Polars::Int32, endianness: "little"
    #     )
    #   )
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────────────────────┬─────────┐
    #   # │ data                ┆ bin2int │
    #   # │ ---                 ┆ ---     │
    #   # │ binary              ┆ i32     │
    #   # ╞═════════════════════╪═════════╡
    #   # │ b"\x05\x00\x00\x00" ┆ 5       │
    #   # │ b"\x10\x00\x01\x00" ┆ 65552   │
    #   # └─────────────────────┴─────────┘
    def reinterpret(
      dtype:,
      endianness: "little"
    )
      dtype = Utils.parse_into_datatype_expr(dtype)

      Utils.wrap_expr(
        _rbexpr.bin_reinterpret(dtype._rbdatatype_expr, endianness)
      )
    end
  end
end

module Polars
  # Namespace for categorical related expressions.
  class CatExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Get the categories stored in this data type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::Series.new(
    #     "cats", ["foo", "bar", "foo", "foo", "ham"], dtype: Polars::Categorical
    #   ).to_frame
    #   df.select(Polars.col("cats").cat.get_categories)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ cats │
    #   # │ ---  │
    #   # │ str  │
    #   # ╞══════╡
    #   # │ foo  │
    #   # │ bar  │
    #   # │ ham  │
    #   # └──────┘
    def get_categories
      Utils.wrap_expr(_rbexpr.cat_get_categories)
    end

    # Return the byte-length of the string representation of each value.
    #
    # @return [Expr]
    #
    # @note
    #   When working with non-ASCII text, the length in bytes is not the same as the
    #   length in characters. You may want to use `len_chars` instead.
    #   Note that `len_bytes` is much more performant (_O(1)_) than
    #   `len_chars` (_O(n)_).
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => Polars::Series.new(["Café", "345", "東京", nil], dtype: Polars::Categorical)}
    #   )
    #   df.with_columns(
    #     Polars.col("a").cat.len_bytes.alias("n_bytes"),
    #     Polars.col("a").cat.len_chars.alias("n_chars")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬─────────┬─────────┐
    #   # │ a    ┆ n_bytes ┆ n_chars │
    #   # │ ---  ┆ ---     ┆ ---     │
    #   # │ cat  ┆ u32     ┆ u32     │
    #   # ╞══════╪═════════╪═════════╡
    #   # │ Café ┆ 5       ┆ 4       │
    #   # │ 345  ┆ 3       ┆ 3       │
    #   # │ 東京 ┆ 6       ┆ 2       │
    #   # │ null ┆ null    ┆ null    │
    #   # └──────┴─────────┴─────────┘
    def len_bytes
      Utils.wrap_expr(_rbexpr.cat_len_bytes)
    end

    # Return the number of characters of the string representation of each value.
    #
    # @return [Expr]
    #
    # @note
    #   When working with ASCII text, use `len_bytes` instead to achieve
    #   equivalent output with much better performance:
    #   `len_bytes` runs in _O(1)_, while `len_chars` runs in (_O(n)_).
    #
    #   A character is defined as a [Unicode scalar value](https://www.unicode.org/glossary/#unicode_scalar_value). A single character is
    #   represented by a single byte when working with ASCII text, and a maximum of
    #   4 bytes otherwise.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"a" => Polars::Series.new(["Café", "345", "東京", nil], dtype: Polars::Categorical)}
    #   )
    #   df.with_columns(
    #     Polars.col("a").cat.len_chars.alias("n_chars"),
    #     Polars.col("a").cat.len_bytes.alias("n_bytes")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬─────────┬─────────┐
    #   # │ a    ┆ n_chars ┆ n_bytes │
    #   # │ ---  ┆ ---     ┆ ---     │
    #   # │ cat  ┆ u32     ┆ u32     │
    #   # ╞══════╪═════════╪═════════╡
    #   # │ Café ┆ 4       ┆ 5       │
    #   # │ 345  ┆ 3       ┆ 3       │
    #   # │ 東京 ┆ 2       ┆ 6       │
    #   # │ null ┆ null    ┆ null    │
    #   # └──────┴─────────┴─────────┘
    def len_chars
      Utils.wrap_expr(_rbexpr.cat_len_chars)
    end

    # Check if string representations of values start with a substring.
    #
    # @param prefix [String]
    #   Prefix substring.
    #
    # @return [Expr]
    #
    # @note
    #   Whereas `str.starts_with` allows expression inputs, `cat.starts_with` requires
    #   a literal string value.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"fruits" => Polars::Series.new(["apple", "mango", nil], dtype: Polars::Categorical)}
    #   )
    #   df.with_columns(
    #     Polars.col("fruits").cat.starts_with("app").alias("has_prefix")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬────────────┐
    #   # │ fruits ┆ has_prefix │
    #   # │ ---    ┆ ---        │
    #   # │ cat    ┆ bool       │
    #   # ╞════════╪════════════╡
    #   # │ apple  ┆ true       │
    #   # │ mango  ┆ false      │
    #   # │ null   ┆ null       │
    #   # └────────┴────────────┘
    #
    # @example Using `starts_with` as a filter condition:
    #   df.filter(Polars.col("fruits").cat.starts_with("app"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌────────┐
    #   # │ fruits │
    #   # │ ---    │
    #   # │ cat    │
    #   # ╞════════╡
    #   # │ apple  │
    #   # └────────┘
    def starts_with(prefix)
      if !prefix.is_a?(::String)
        msg = "'prefix' must be a string; found #{prefix.inspect}"
        raise TypeError, msg
      end
      Utils.wrap_expr(_rbexpr.cat_starts_with(prefix))
    end

    # Check if string representations of values end with a substring.
    #
    # @param suffix [String]
    #   Suffix substring.
    #
    # @return [Expr]
    #
    # @note
    #   Whereas `str.ends_with` allows expression inputs, `cat.ends_with` requires a
    #   literal string value.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"fruits" => Polars::Series.new(["apple", "mango", nil], dtype: Polars::Categorical)}
    #   )
    #   df.with_columns(Polars.col("fruits").cat.ends_with("go").alias("has_suffix"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬────────────┐
    #   # │ fruits ┆ has_suffix │
    #   # │ ---    ┆ ---        │
    #   # │ cat    ┆ bool       │
    #   # ╞════════╪════════════╡
    #   # │ apple  ┆ false      │
    #   # │ mango  ┆ true       │
    #   # │ null   ┆ null       │
    #   # └────────┴────────────┘
    #
    # @example Using `ends_with` as a filter condition:
    #   df.filter(Polars.col("fruits").cat.ends_with("go"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌────────┐
    #   # │ fruits │
    #   # │ ---    │
    #   # │ cat    │
    #   # ╞════════╡
    #   # │ mango  │
    #   # └────────┘
    def ends_with(suffix)
      if !suffix.is_a?(::String)
        msg = "'suffix' must be a string; found #{suffix.inspect}"
        raise TypeError, msg
      end
      Utils.wrap_expr(_rbexpr.cat_ends_with(suffix))
    end

    # Extract a substring from the string representation of each value.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the string.
    #
    # @return [Expr]
    #
    # @note
    #   Both the `offset` and `length` inputs are defined in terms of the number
    #   of characters in the (UTF8) string. A character is defined as a
    #   [Unicode scalar value](https://www.unicode.org/glossary/#unicode_scalar_value). A single character is represented by a single byte
    #   when working with ASCII text, and a maximum of 4 bytes otherwise.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "s" => Polars::Series.new(
    #         ["pear", nil, "papaya", "dragonfruit"],
    #         dtype: Polars::Categorical
    #       )
    #     }
    #   )
    #   df.with_columns(Polars.col("s").cat.slice(-3).alias("slice"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────┬───────┐
    #   # │ s           ┆ slice │
    #   # │ ---         ┆ ---   │
    #   # │ cat         ┆ str   │
    #   # ╞═════════════╪═══════╡
    #   # │ pear        ┆ ear   │
    #   # │ null        ┆ null  │
    #   # │ papaya      ┆ aya   │
    #   # │ dragonfruit ┆ uit   │
    #   # └─────────────┴───────┘
    #
    # @example Using the optional `length` parameter
    #   df.with_columns(Polars.col("s").cat.slice(4, 3).alias("slice"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────┬───────┐
    #   # │ s           ┆ slice │
    #   # │ ---         ┆ ---   │
    #   # │ cat         ┆ str   │
    #   # ╞═════════════╪═══════╡
    #   # │ pear        ┆       │
    #   # │ null        ┆ null  │
    #   # │ papaya      ┆ ya    │
    #   # │ dragonfruit ┆ onf   │
    #   # └─────────────┴───────┘
    def slice(offset, length = nil)
      Utils.wrap_expr(_rbexpr.cat_slice(offset, length))
    end
  end
end

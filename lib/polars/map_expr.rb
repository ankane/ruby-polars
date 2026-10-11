module Polars
  # Namespace for map related expressions.
  class MapExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Convert the `Map` to a `List` of `Struct` entries.
    #
    # Each entry is a `Struct` with a `key` and a `value` field. Entry order is
    # preserved. The inverse of :meth:`Expr.list.to_map`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"m" => Polars::Series.new([{"a" => 1, "b" => 2}], dtype: Polars::Map.new(Polars::String, Polars::Int64))}
    #   )
    #   df.select(Polars.col("m").map.entries)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌────────────────────┐
    #   # │ m                  │
    #   # │ ---                │
    #   # │ list[struct[2]]    │
    #   # ╞════════════════════╡
    #   # │ [{"a",1}, {"b",2}] │
    #   # └────────────────────┘
    def entries
      Utils.wrap_expr(_rbexpr.map_entries)
    end

    # Get the keys of every map as a `List`, in entry order.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "m" => Polars::Series.new(
    #         [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #       )
    #     }
    #   )
    #   df.select(Polars.col("m").map.keys)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌────────────┐
    #   # │ m          │
    #   # │ ---        │
    #   # │ list[str]  │
    #   # ╞════════════╡
    #   # │ ["a", "b"] │
    #   # │ []         │
    #   # │ null       │
    #   # └────────────┘
    def keys
      Utils.wrap_expr(_rbexpr.map_keys)
    end

    # Get the values of every map as a `List`, in entry order.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "m" => Polars::Series.new(
    #         [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #       )
    #     }
    #   )
    #   df.select(Polars.col("m").map.values)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────────┐
    #   # │ m         │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2]    │
    #   # │ []        │
    #   # │ null      │
    #   # └───────────┘
    def values
      Utils.wrap_expr(_rbexpr.map_values)
    end

    # Get the number of entries of every map.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "m" => Polars::Series.new(
    #         [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #       )
    #     }
    #   )
    #   df.select(Polars.col("m").map.len)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ m    │
    #   # │ ---  │
    #   # │ u32  │
    #   # ╞══════╡
    #   # │ 2    │
    #   # │ 0    │
    #   # │ null │
    #   # └──────┘
    def len
      Utils.wrap_expr(_rbexpr.map_len)
    end

    # Check whether every map holds `key`.
    #
    # @param key [Object]
    #   Key to look up. A single value is broadcast over all maps. It is cast to
    #   the map's key type, and a key the cast cannot represent exactly, because it
    #   is out of range or would be rounded, is absent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "m" => Polars::Series.new(
    #         [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #       )
    #     }
    #   )
    #   df.select(Polars.col("m").map.contains_key("a"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ m     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ true  │
    #   # │ false │
    #   # │ null  │
    #   # └───────┘
    def contains_key(key)
      key_rbexpr = Utils.parse_into_expression(key, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.map_contains_key(key_rbexpr))
    end

    # Look up `key` in every map.
    #
    # Maps that do not hold `key` yield null, as do null maps. Use
    # `contains_key` to tell a missing key apart from a key whose value is null.
    #
    # @param key [Object]
    #   Key to look up. A single value is broadcast over all maps. It is cast to
    #   the map's key type, and a key the cast cannot represent exactly, because it
    #   is out of range or would be rounded, is absent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "m" => Polars::Series.new(
    #         [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #       ),
    #       "k" => ["b", "a", "a"]
    #     }
    #   )
    #   df.select(
    #     Polars.col("m").map.get("a").alias("a"),
    #     Polars.col("m").map.get(Polars.col("k")).alias("by_col")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬────────┐
    #   # │ a    ┆ by_col │
    #   # │ ---  ┆ ---    │
    #   # │ i64  ┆ i64    │
    #   # ╞══════╪════════╡
    #   # │ 1    ┆ 2      │
    #   # │ null ┆ null   │
    #   # │ null ┆ null   │
    #   # └──────┴────────┘
    def get(key)
      key_rbexpr = Utils.parse_into_expression(key, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.map_get(key_rbexpr))
    end
  end
end

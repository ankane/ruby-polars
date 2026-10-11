module Polars
  # Series.map namespace.
  class MapNameSpace
    include ExprDispatch

    self._accessor = "map"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Convert the `Map` to a `List` of `Struct` entries.
    #
    # Each entry is a `Struct` with a `key` and a `value` field. Entry order is
    # preserved. The inverse of :meth:`Expr.list.to_map`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([{"a" => 1, "b" => 2}], dtype: Polars::Map.new(Polars::String, Polars::Int64))
    #   s.map.entries
    #   # =>
    #   # shape: (1,)
    #   # Series: '' [list[struct[2]]]
    #   # [
    #   #         [{"a",1}, {"b",2}]
    #   # ]
    def entries
      super
    end

    # Get the keys of every map as a `List`, in entry order.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #   )
    #   s.map.keys
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [list[str]]
    #   # [
    #   #         ["a", "b"]
    #   #         []
    #   #         null
    #   # ]
    def keys
      super
    end

    # Get the values of every map as a `List`, in entry order.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #   )
    #   s.map.values
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [list[i64]]
    #   # [
    #   #         [1, 2]
    #   #         []
    #   #         null
    #   # ]
    def values
      super
    end

    # Get the number of entries of every map.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #   )
    #   s.map.len
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [u32]
    #   # [
    #   #         2
    #   #         0
    #   #         null
    #   # ]
    def len
      super
    end

    # Check whether every map holds `key`.
    #
    # @param key [Object]
    #   Key to look up. A single value is broadcast over all maps.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #   )
    #   s.map.contains_key("a")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         null
    #   # ]
    def contains_key(key)
      super
    end

    # Look up `key` in every map.
    #
    # Maps that do not hold `key` yield null, as do null maps. Use
    # `contains_key` to tell a missing key apart from a key whose value is null.
    #
    # @param key [Object]
    #   Key to look up. A single value is broadcast over all maps.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(
    #     [{"a" => 1, "b" => 2}, {}, nil], dtype: Polars::Map.new(Polars::String, Polars::Int64)
    #   )
    #   s.map.get("a")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [i64]
    #   # [
    #   #         1
    #   #         null
    #   #         null
    #   # ]
    def get(key)
      super
    end
  end
end

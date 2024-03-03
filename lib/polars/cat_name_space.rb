module Polars
  # Series.cat namespace.
  class CatNameSpace
    include ExprDispatch

    self._accessor = "cat"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Determine how this categorical series should be sorted.
    #
    # @param ordering ["physical", "lexical"]
    #   Ordering type:
    #
    #   - 'physical' -> Use the physical representation of the categories to
    #       determine the order (default).
    #   - 'lexical' -> Use the string values to determine the ordering.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"cats" => ["z", "z", "k", "a", "b"], "vals" => [3, 1, 2, 2, 3]}
    #   ).with_columns(
    #     [
    #       Polars.col("cats").cast(:cat).cat.set_ordering("lexical")
    #     ]
    #   )
    #   df.sort(["cats", "vals"])
    #   # =>
    #   # shape: (5, 2)
    #   # ┌──────┬──────┐
    #   # │ cats ┆ vals │
    #   # │ ---  ┆ ---  │
    #   # │ cat  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ a    ┆ 2    │
    #   # │ b    ┆ 3    │
    #   # │ k    ┆ 2    │
    #   # │ z    ┆ 1    │
    #   # │ z    ┆ 3    │
    #   # └──────┴──────┘
    def set_ordering(ordering)
      super
    end

    # Get the categories stored in this data type.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["foo", "bar", "foo", "foo", "ham"], dtype: Polars::Categorical)
    #   s.cat.get_categories
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [str]
    #   # [
    #   #         "foo"
    #   #         "bar"
    #   #         "ham"
    #   # ]
    def get_categories
      super
    end

    # Return whether or not the column is a local categorical.
    #
    # @return [Boolean]
    #
    # @example Categoricals constructed without a string cache are considered local.
    #   s = Polars::Series.new(["a", "b", "a"], dtype: Polars::Categorical)
    #   s.cat.is_local
    #   # => true
    #
    # @example Categoricals constructed with a string cache are considered global.
    #   s = nil
    #   Polars::StringCache.new do
    #     s = Polars::Series.new(["a", "b", "a"], dtype: Polars::Categorical)
    #   end
    #   s.cat.is_local
    #   # => false
    def is_local
      _s.cat_is_local
    end

    # Convert a categorical column to its local representation.
    #
    # This may change the underlying physical representation of the column.
    #
    # @return [Series]
    #
    # @example Compare the global and local representations of a categorical.
    #   s = nil
    #   Polars::StringCache.new do
    #     _ = Polars::Series.new("x", ["a", "b", "a"], dtype: Polars::Categorical)
    #     s = Polars::Series.new("y", ["c", "b", "d"], dtype: Polars::Categorical)
    #   end
    #   s.to_physical
    #   # =>
    #   # shape: (3,)
    #   # Series: 'y' [u32]
    #   # [
    #   #         2
    #   #         1
    #   #         3
    #   # ]
    #
    # @example
    #   s.cat.to_local.to_physical
    #   # =>
    #   # shape: (3,)
    #   # Series: 'y' [u32]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   # ]
    def to_local
      Utils.wrap_s(_s.cat_to_local)
    end
  end
end

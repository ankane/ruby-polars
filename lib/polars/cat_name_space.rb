module Polars
  # Series.cat namespace.
  class CatNameSpace
    include ExprDispatch

    self._accessor = "cat"

    # @private
    def initialize(series)
      self._s = series._s
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
    # Always returns false.
    #
    # @deprecated
    #   `cat.is_local` is deprecated; Categoricals no longer have a local scope.
    #
    # @return [Boolean]
    def is_local
      _s.cat_is_local
    end

    # Simply returns the column as-is, local representations are deprecated.
    #
    # @return [Series]
    def to_local
      Utils.wrap_s(_s.cat_to_local)
    end

    # Indicate whether the Series uses lexical ordering.
    #
    # @deprecated
    #   `cat.uses_lexical_ordering` is deprecated; Categoricals are now always ordered lexically.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new(["b", "a", "b"]).cast(Polars::Categorical)
    #   s.cat.uses_lexical_ordering
    #   # => true
    def uses_lexical_ordering
      _s.cat_uses_lexical_ordering
    end

    # Return the byte-length of the string representation of each value.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["Café", "345", "東京", nil], dtype: Polars::Categorical)
    #   s.cat.len_bytes
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         5
    #   #         3
    #   #         6
    #   #         null
    #   # ]
    def len_bytes
      super
    end

    # Return the number of characters of the string representation of each value.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["Café", "345", "東京", nil], dtype: Polars::Categorical)
    #   s.cat.len_chars
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         4
    #   #         3
    #   #         2
    #   #         null
    #   # ]
    def len_chars
      super
    end

    # Check if string representations of values start with a substring.
    #
    # @param prefix [String]
    #     Prefix substring.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("fruits", ["apple", "mango", nil], dtype: Polars::Categorical)
    #   s.cat.starts_with("app")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'fruits' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         null
    #   # ]
    def starts_with(prefix)
      super
    end

    # Check if string representations of values end with a substring.
    #
    # @param suffix [String]
    #   Suffix substring.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("fruits", ["apple", "mango", nil], dtype: Polars::Categorical)
    #   s.cat.ends_with("go")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'fruits' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         null
    #   # ]
    def ends_with(suffix)
      super
    end

    # Extract a substring from the string representation of each string value.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the string.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["pear", nil, "papaya", "dragonfruit"], dtype: Polars::Categorical)
    #   s.cat.slice(-3)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [str]
    #   # [
    #   #         "ear"
    #   #         null
    #   #         "aya"
    #   #         "uit"
    #   # ]
    #
    # @example Using the optional `length` parameter
    #   s.cat.slice(4, 3)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [str]
    #   # [
    #   #         ""
    #   #         null
    #   #         "ya"
    #   #         "onf"
    #   # ]
    def slice(offset, length = nil)
      super
    end
  end
end

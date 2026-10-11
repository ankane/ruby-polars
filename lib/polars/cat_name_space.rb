module Polars
  # Series.cat namespace.
  class CatNameSpace
    include ExprDispatch

    self._accessor = "cat"

    # @private
    def initialize(series)
      self._s = series._s
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

    # Create a Series with a categorical or enum `dtype`.
    #
    # The input series must be the physical type of the categorical or enum dtype.
    #
    # @param dtype [Object]
    #   The target categorical or enum dtype.
    # @param strict [Boolean]
    #   Whether to panic when encountering an illegal category.
    #
    # @return [Series]
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    def to(dtype, strict: true)
      super
    end

    # Get the physical values of a Series with a categorical or enum data type.
    #
    # @return [Series]
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    def physical
      super
    end
  end
end

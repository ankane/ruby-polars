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
  end
end

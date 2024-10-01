module Polars
  # Class for enabling and disabling the global string cache.
  #
  # @example Construct two Series using the same global string cache.
  #   s1 = nil
  #   s2 = nil
  #   Polars::StringCache.new do
  #     s1 = Polars::Series.new("color", ["red", "green", "red"], dtype: Polars::Categorical)
  #     s2 = Polars::Series.new("color", ["blue", "red", "green"], dtype: Polars::Categorical)
  #   end
  #
  # @example As both Series are constructed under the same global string cache, they can be concatenated.
  #   Polars.concat([s1, s2])
  #   # =>
  #   # shape: (6,)
  #   # Series: 'color' [cat]
  #   # [
  #   #         "red"
  #   #         "green"
  #   #         "red"
  #   #         "blue"
  #   #         "red"
  #   #         "green"
  #   # ]
  class StringCache
    def initialize(&block)
      RbStringCacheHolder.hold(&block)
    end
  end

  module Functions
    # Enable the global string cache.
    #
    # `Categorical` columns created under the same global string cache have
    # the same underlying physical value when string values are equal. This allows the
    # columns to be concatenated or used in a join operation, for example.
    #
    # @return [nil]
    #
    # @example Construct two Series using the same global string cache.
    #   Polars.enable_string_cache
    #   s1 = Polars::Series.new("color", ["red", "green", "red"], dtype: Polars::Categorical)
    #   s2 = Polars::Series.new("color", ["blue", "red", "green"], dtype: Polars::Categorical)
    #   Polars.disable_string_cache
    #
    # @example As both Series are constructed under the same global string cache, they can be concatenated.
    #   Polars.concat([s1, s2])
    #   # =>
    #   # shape: (6,)
    #   # Series: 'color' [cat]
    #   # [
    #   #         "red"
    #   #         "green"
    #   #         "red"
    #   #         "blue"
    #   #         "red"
    #   #         "green"
    #   # ]
    def enable_string_cache
      Plr.enable_string_cache
    end

    # Disable and clear the global string cache.
    #
    # @return [nil]
    #
    # @example Construct two Series using the same global string cache.
    #   Polars.enable_string_cache
    #   s1 = Polars::Series.new("color", ["red", "green", "red"], dtype: Polars::Categorical)
    #   s2 = Polars::Series.new("color", ["blue", "red", "green"], dtype: Polars::Categorical)
    #   Polars.disable_string_cache
    #
    # @example As both Series are constructed under the same global string cache, they can be concatenated.
    #   Polars.concat([s1, s2])
    #   # =>
    #   # shape: (6,)
    #   # Series: 'color' [cat]
    #   # [
    #   #         "red"
    #   #         "green"
    #   #         "red"
    #   #         "blue"
    #   #         "red"
    #   #         "green"
    #   # ]
    def disable_string_cache
      Plr.disable_string_cache
    end

    # Check whether the global string cache is enabled.
    #
    # @return [Boolean]
    def using_string_cache
      Plr.using_string_cache
    end
  end
end

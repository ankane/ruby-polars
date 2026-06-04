module Polars
  # Does nothing.
  #
  # @deprecated
  #   The string cache was used to maintain the mapping for the Categorical
  #   dtype, this is now done through `Polars::Categories`.
  class StringCache
    def initialize(&block)
    end
  end

  def self.string_cache(...)
    StringCache.new(...)
  end

  module Functions
    # Does nothing.
    #
    # @deprecated
    #   The string cache was used to maintain the mapping for the Categorical
    #   dtype, this is now done through `Polars::Categories`.
    #
    # @return [nil]
    def enable_string_cache
    end

    # Does nothing.
    #
    # @deprecated
    #     The string cache was used to maintain the mapping for the Categorical
    #     dtype, this is now done through `Polars::Categories`.
    #
    # @return [nil]
    def disable_string_cache
    end

    # Always returns true.
    #
    # @deprecated
    #   The string cache was used to maintain the mapping for the Categorical
    #   dtype, this is now done through `Polars::Categories`.
    #
    # @return [Boolean]
    def using_string_cache
      true
    end
  end
end

module Polars
  module Functions
    # Set the global random seed for Polars.
    #
    # This random seed is used to determine things such as shuffle ordering.
    #
    # @param seed [Integer]
    #   A non-negative integer < 2**64 used to seed the internal global
    #   random number generator.
    #
    # @return [nil]
    def set_random_seed(seed)
      Plr.set_random_seed(seed)
    end
  end
end

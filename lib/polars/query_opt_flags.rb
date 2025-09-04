module Polars
  # The set of the optimizations considered during query optimization.
  #
  # @note
  #   This functionality is considered **unstable**. It may be changed
  #   at any point without it being considered a breaking change.
  class QueryOptFlags
    def initialize
      @_rboptflags = RbOptFlags.default
    end
  end
end

module Polars
  module LazyFunctions
    # Aggregate to list.
    #
    # @return [Expr]
    def to_list(name)
      col(name).list
    end
  end
end

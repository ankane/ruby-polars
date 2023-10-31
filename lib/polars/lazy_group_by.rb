module Polars
  # Created by `df.lazy.group_by("foo")`.
  class LazyGroupBy
    # @private
    def initialize(lgb)
      @lgb = lgb
    end

    # Describe the aggregation that need to be done on a group.
    #
    # @return [LazyFrame]
    def agg(aggs)
      rbexprs = Utils.selection_to_rbexpr_list(aggs)
      Utils.wrap_ldf(@lgb.agg(rbexprs))
    end

    # Get the first `n` rows of each group.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["c", "c", "a", "c", "a", "b"],
    #       "nrs" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   df.group_by("letters").head(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # │ c       ┆ 1   │
    #   # │ c       ┆ 2   │
    #   # └─────────┴─────┘
    def head(n = 5)
      Utils.wrap_ldf(@lgb.head(n))
    end

    # Get the last `n` rows of each group.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["c", "c", "a", "c", "a", "b"],
    #       "nrs" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   df.group_by("letters").tail(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # │ c       ┆ 2   │
    #   # │ c       ┆ 4   │
    #   # └─────────┴─────┘
    def tail(n = 5)
      Utils.wrap_ldf(@lgb.tail(n))
    end

    # def apply
    # end
  end
end

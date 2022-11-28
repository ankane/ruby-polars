module Polars
  class LazyGroupBy
    # @private
    def initialize(lgb, lazyframe_class)
      @lgb = lgb
      @lazyframe_class = lazyframe_class
    end

    # Describe the aggregation that need to be done on a group.
    #
    # @return [LazyFrame]
    def agg(aggs)
      rbexprs = Utils.selection_to_rbexpr_list(aggs)
      @lazyframe_class._from_rbldf(@lgb.agg(rbexprs))
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
    #   df.groupby("letters").head(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ a       ┆ 5   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b       ┆ 6   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c       ┆ 1   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c       ┆ 2   │
    #   # └─────────┴─────┘
    def head(n = 5)
      @lazyframe_class._from_rbldf(@lgb.head(n))
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
    #   df.groupby("letters").tail(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ a       ┆ 5   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b       ┆ 6   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c       ┆ 2   │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c       ┆ 4   │
    #   # └─────────┴─────┘
    def tail(n = 5)
      @lazyframe_class._from_rbldf(@lgb.tail(n))
    end

    # def apply
    # end
  end
end

module Polars
  # The set of the optimizations considered during query optimization.
  #
  # @note
  #   This functionality is considered **unstable**. It may be changed
  #   at any point without it being considered a breaking change.
  class QueryOptFlags
    def initialize(
      predicate_pushdown: nil,
      projection_pushdown: nil,
      simplify_expression: nil,
      slice_pushdown: nil,
      comm_subplan_elim: nil,
      comm_subexpr_elim: nil,
      cluster_with_columns: nil,
      collapse_joins: nil,
      check_order_observe: nil,
      fast_projection: nil
    )
      @_rboptflags = RbOptFlags.default
      update(
        predicate_pushdown: predicate_pushdown,
        projection_pushdown: projection_pushdown,
        simplify_expression: simplify_expression,
        slice_pushdown: slice_pushdown,
        comm_subplan_elim: comm_subplan_elim,
        comm_subexpr_elim: comm_subexpr_elim,
        cluster_with_columns: cluster_with_columns,
        collapse_joins: collapse_joins,
        check_order_observe: check_order_observe,
        fast_projection: fast_projection
      )
    end

    def update(
      predicate_pushdown: nil,
      projection_pushdown: nil,
      simplify_expression: nil,
      slice_pushdown: nil,
      comm_subplan_elim: nil,
      comm_subexpr_elim: nil,
      cluster_with_columns: nil,
      collapse_joins: nil,
      check_order_observe: nil,
      fast_projection: nil
    )
      raise Todo
    end
  end
end

module Polars
  # The set of the optimizations considered during query optimization.
  #
  # @note
  #   This functionality is considered **unstable**. It may be changed
  #   at any point without it being considered a breaking change.
  class QueryOptFlags
    # @private
    attr_accessor :_rboptflags

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
      self._rboptflags = RbOptFlags.default
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
      if !predicate_pushdown.nil?
        self.predicate_pushdown = predicate_pushdown
      end
      if !projection_pushdown.nil?
        self.projection_pushdown = projection_pushdown
      end
      if !simplify_expression.nil?
        self.simplify_expression = simplify_expression
      end
      if !slice_pushdown.nil?
        self.slice_pushdown = slice_pushdown
      end
      if !comm_subplan_elim.nil?
        self.comm_subplan_elim = comm_subplan_elim
      end
      if !comm_subexpr_elim.nil?
        self.comm_subexpr_elim = comm_subexpr_elim
      end
      if !cluster_with_columns.nil?
        self.cluster_with_columns = cluster_with_columns
      end
      if !collapse_joins.nil?
        Utils.issue_deprecation_warning(
          "the `collapse_joins` parameter for `QueryOptFlags` is deprecated. " +
          "Use `predicate_pushdown` instead."
        )
        if !collapse_joins
          self.predicate_pushdown = false
        end
      end
      if !check_order_observe.nil?
        self.check_order_observe = check_order_observe
      end
      if !fast_projection.nil?
        self.fast_projection = fast_projection
      end

      self
    end

    def no_optimizations
      _rboptflags.no_optimizations
    end

    def projection_pushdown
      _rboptflags.get_projection_pushdown
    end

    def projection_pushdown=(value)
      _rboptflags.set_projection_pushdown(value)
    end

    def predicate_pushdown
      _rboptflags.get_predicate_pushdown
    end

    def predicate_pushdown=(value)
      _rboptflags.set_predicate_pushdown(value)
    end
  end

  DEFAULT_QUERY_OPT_FLAGS = QueryOptFlags.new
end

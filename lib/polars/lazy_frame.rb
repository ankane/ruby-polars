module Polars
  class LazyFrame
    attr_accessor :_ldf

    def self._from_rbldf(rb_ldf)
      ldf = LazyFrame.allocate
      ldf._ldf = rb_ldf
      ldf
    end

    def collect(
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      string_cache: false,
      no_optimization: false,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
      end

      if allow_streaming
        common_subplan_elimination = false
      end

      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming
      )
      Utils.wrap_df(ldf.collect)
    end

    def filter(predicate)
      _from_rbldf(
        _ldf.filter(
          Utils.expr_to_lit_or_expr(predicate, str_to_lit: false)._rbexpr
        )
      )
    end

    def select(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      _from_rbldf(_ldf.select(exprs))
    end

    def groupby(by, maintain_order: false)
      rbexprs_by = Utils.selection_to_rbexpr_list(by)
      lgb = _ldf.groupby(rbexprs_by, maintain_order)
      LazyGroupBy.new(lgb, self.class)
    end

    def join(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      how: "inner",
      suffix: "_right",
      allow_parallel: true,
      force_parallel: false
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if how == "cross"
        return _from_rbldf(
          _ldf.join(
            other._ldf, [], [], allow_parallel, force_parallel, how, suffix
          )
        )
      end

      if !on.nil?
        rbexprs = Utils.selection_to_rbexpr_list(on)
        rbexprs_left = rbexprs
        rbexprs_right = rbexprs
      elsif !left_on.nil? && !right_on.nil?
        rbexprs_left = Utils.selection_to_rbexpr_list(left_on)
        rbexprs_right = Utils.selection_to_rbexpr_list(right_on)
      else
        raise ArgumentError, "must specify `on` OR `left_on` and `right_on`"
      end

      _from_rbldf(
        self._ldf.join(
          other._ldf,
          rbexprs_left,
          rbexprs_right,
          allow_parallel,
          force_parallel,
          how,
          suffix,
        )
      )
    end

    def with_columns(exprs)
      exprs =
        if exprs.nil?
          []
        elsif exprs.is_a?(Expr)
          [exprs]
        else
          exprs.to_a
        end

      rbexprs = []
      exprs.each do |e|
        case e
        when Expr
          rbexprs << e._rbexpr
        when Series
          rbexprs = Utils.lit(e)._rbexpr
        else
          raise ArgumentError, "Expected an expression, got #{e}"
        end
      end

      _from_rbldf(_ldf.with_columns(rbexprs))
    end

    def with_column(column)
      with_columns([column])
    end

    def rename(mapping)
      existing = mapping.keys
      _new = mapping.values
      _from_rbldf(_ldf.rename(existing, _new))
    end

    def fill_nan(fill_value)
      if !fill_value.is_a?(Expr)
        fill_value = Utils.lit(fill_value)
      end
      _from_rbldf(_ldf.fill_nan(fill_value._rbexpr))
    end

    private

    def _from_rbldf(rb_ldf)
      self.class._from_rbldf(rb_ldf)
    end
  end
end

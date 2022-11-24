module Polars
  module LazyFunctions
    def col(name)
      name = name.to_s if name.is_a?(Symbol)
      Utils.wrap_expr(RbExpr.col(name))
    end

    def element
      col("")
    end

    def std(column, ddof: 1)
      if column.is_a?(Series)
        column.std(ddof: ddof)
      else
        col(column).std(ddof: ddof)
      end
    end

    def var(column, ddof: 1)
      if column.is_a?(Series)
        column.var(ddof: ddof)
      else
        col(column).var(ddof: ddof)
      end
    end

    def max(column)
      if column.is_a?(Series)
        column.max
      elsif column.is_a?(String) || column.is_a?(Symbol)
        col(column).max
      else
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_max_exprs(exprs))
      end
    end

    def min(column)
      if column.is_a?(Series)
        column.min
      elsif column.is_a?(String) || column.is_a?(Symbol)
        col(column).min
      else
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_min_exprs(exprs))
      end
    end

    def sum(column)
      if column.is_a?(Series)
        column.sum
      elsif column.is_a?(String) || column.is_a?(Symbol)
        col(column.to_s).sum
      elsif column.is_a?(Array)
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_sum_exprs(exprs))
      else
        raise "todo"
      end
    end

    def mean(column)
      if column.is_a?(Series)
        column.mean
      else
        col(column).mean
      end
    end

    def avg(column)
      mean(column)
    end

    def median(column)
      if column.is_a?(Series)
        column.median
      else
        col(column).median
      end
    end

    def lit(value)
      Utils.wrap_expr(RbExpr.lit(value))
    end

    def arange(low, high, step: 1, eager: false, dtype: nil)
      low = Utils.expr_to_lit_or_expr(low, str_to_lit: false)
      high = Utils.expr_to_lit_or_expr(high, str_to_lit: false)
      range_expr = Utils.wrap_expr(RbExpr.arange(low._rbexpr, high._rbexpr, step))

      if !dtype.nil? && dtype != "i64"
        range_expr = range_expr.cast(dtype)
      end

      if !eager
        range_expr
      else
        DataFrame.new
          .select(range_expr)
          .to_series
          .rename("arange", in_place: true)
      end
    end

    def all(name = nil)
      if name.nil?
        col("*")
      elsif name.is_a?(String) || name.is_a?(Symbol)
        col(name).all
      else
        raise "todo"
      end
    end

    def when(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      pw = RbExpr.when(expr._rbexpr)
      When.new(pw)
    end
  end
end

module Polars
  module LazyFunctions
    def col(name)
      if name.is_a?(Series)
        name = name.to_a
      end

      if name.is_a?(Array)
        if name.length == 0 || name[0].is_a?(String) || name[0].is_a?(Symbol)
          name = name.map { |v| v.is_a?(Symbol) ? v.to_s : v }
          Utils.wrap_expr(RbExpr.cols(name))
        elsif Utils.is_polars_dtype(name[0])
          raise Todo
          # Utils.wrap_expr(_dtype_cols(name))
        else
          raise ArgumentError, "Expected list values to be all `str` or all `DataType`"
        end
      else
        name = name.to_s if name.is_a?(Symbol)
        Utils.wrap_expr(RbExpr.col(name))
      end
    end

    def element
      col("")
    end

    def count(column = nil)
      if column.nil?
        return Utils.wrap_expr(RbExpr.count)
      end

      if column.is_a?(Series)
        column.len
      else
        col(column).count
      end
    end

    # def to_list
    # end

    #
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
        raise Todo
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

    # def n_unique
    # end

    #
    def first(column = nil)
      if column.nil?
        return Utils.wrap_expr(RbExpr.first)
      end

      if column.is_a?(Series)
        if column.len > 0
          column[0]
        else
          raise IndexError, "The series is empty, so no first value can be returned."
        end
      else
        col(column).first
      end
    end

    # def last
    # end

    # def head
    # end

    # def tail
    # end

    #
    def lit(value)
      if value.is_a?(Polars::Series)
        name = value.name
        value = value._s
        e = Utils.wrap_expr(RbExpr.lit(value))
        if name == ""
          return e
        end
        return e.alias(name)
      end

      Utils.wrap_expr(RbExpr.lit(value))
    end

    # def cumsum
    # end

    # def spearman_rank_corr
    # end

    # def pearson_corr
    # end

    # def cov
    # end

    # def map
    # end

    # def apply
    # end

    #
    def fold(acc, f, exprs)
      acc = Utils.expr_to_lit_or_expr(acc, str_to_lit: true)
      if exprs.is_a?(Expr)
        exprs = [exprs]
      end

      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.fold(acc._rbexpr, f, exprs))
    end

    # def reduce
    # end

    # def cumfold
    # end

    # def cumreduce
    # end

    # def any
    # end

    # def exclude
    # end

    #
    def all(name = nil)
      if name.nil?
        col("*")
      elsif name.is_a?(String) || name.is_a?(Symbol)
        col(name).all
      else
        raise Todo
      end
    end

    # def groups
    # end

    # def quantile
    # end

    #
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

    # def argsort_by
    # end

    # def duration
    # end

    # def format
    # end

    #
    def concat_list(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.concat_lst(exprs))
    end

    # def collect_all
    # end

    #
    def select(exprs)
      DataFrame.new([]).select(exprs)
    end

    # def struct
    # end

    # def repeat
    # end

    #
    def arg_where(condition, eager: false)
      if eager
        if !condition.is_a?(Series)
          raise ArgumentError, "expected 'Series' in 'arg_where' if 'eager=True', got #{condition.class.name}"
        end
        condition.to_frame.select(arg_where(Polars.col(condition.name))).to_series
      else
        condition = Utils.expr_to_lit_or_expr(condition, str_to_lit: true)
        Utils.wrap_expr(_arg_where(condition._rbexpr))
      end
    end

    # def coalesce
    # end

    # def from_epoch
    # end

    #
    def when(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      pw = RbExpr.when(expr._rbexpr)
      When.new(pw)
    end
  end
end

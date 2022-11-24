module Polars
  class Expr
    attr_accessor :_rbexpr

    def self._from_rbexpr(rbexpr)
      expr = Expr.allocate
      expr._rbexpr = rbexpr
      expr
    end

    def to_s
      _rbexpr.to_str
    end
    alias_method :inspect, :to_s

    def ^(other)
      wrap_expr(_rbexpr._xor(_to_rbexpr(other)))
    end

    def &(other)
      wrap_expr(_rbexpr._and(_to_rbexpr(other)))
    end

    def |(other)
      wrap_expr(_rbexpr._or(_to_rbexpr(other)))
    end

    # def +(other)
    # end

    # def -(other)
    # end

    def *(other)
      wrap_expr(_rbexpr * _to_rbexpr(other))
    end

    # def /(other)
    # end

    # def %(other)
    # end

    # def **(power)
    # end

    def >=(other)
      wrap_expr(_rbexpr.gt_eq(_to_expr(other)._rbexpr))
    end

    def <=(other)
      wrap_expr(_rbexpr.lt_eq(_to_expr(other)._rbexpr))
    end

    def ==(other)
      wrap_expr(_rbexpr.eq(_to_expr(other)._rbexpr))
    end

    def !=(other)
      wrap_expr(_rbexpr.neq(_to_expr(other)._rbexpr))
    end

    def <(other)
      wrap_expr(_rbexpr.lt(_to_expr(other)._rbexpr))
    end

    def >(other)
      wrap_expr(_rbexpr.gt(_to_expr(other)._rbexpr))
    end

    # def to_physical
    # end

    def any
      wrap_expr(_rbexpr.any)
    end

    def all
      wrap_expr(_rbexpr.all)
    end

    # def sqrt
    # end

    # def log10
    # end

    # def exp
    # end

    def alias(name)
      wrap_expr(_rbexpr._alias(name))
    end

    # def exclude
    # end

    # def keep_name
    # end

    # def prefix
    # end

    def suffix(suffix)
      wrap_expr(_rbexpr.suffix(suffix))
    end

    # def map_alias
    # end

    def is_not
      wrap_expr(_rbexpr.is_not)
    end

    def is_null
      wrap_expr(_rbexpr.is_null)
    end

    def is_not_null
      wrap_expr(_rbexpr.is_not_null)
    end

    # def is_finite
    # end

    # def is_infinite
    # end

    # def is_nan
    # end

    # def is_not_nan
    # end

    # def agg_groups
    # end

    def count
      wrap_expr(_rbexpr.count)
    end

    def len
      count
    end

    # def slice
    # end

    # def append
    # end

    # def rechunk
    # end

    # def drop_nulls
    # end

    # def drop_nans
    # end

    # def cumsum
    # end

    # def cumprod
    # end

    # def cummin
    # end

    # def cummax
    # end

    # def cumcount
    # end

    # def floor
    # end

    # def ceil
    # end

    # def round
    # end

    # def dot
    # end

    # def mode
    # end

    # def cast
    # end

    def sort(reverse: false, nulls_last: false)
      wrap_expr(_rbexpr.sort_with(reverse, nulls_last))
    end

    def sort_by(by, reverse: false)
      if !by.is_a?(Array)
        by = [by]
      end
      if !reverse.is_a?(Array)
        reverse = [reverse]
      end
      by = Utils.selection_to_rbexpr_list(by)

      wrap_expr(_rbexpr.sort_by(by, reverse))
    end

    def fill_null(value = nil, strategy: nil, limit: nil)
      if !value.nil? && !strategy.nil?
        raise ArgumentError, "cannot specify both 'value' and 'strategy'."
      elsif value.nil? && strategy.nil?
        raise ArgumentError, "must specify either a fill 'value' or 'strategy'"
      elsif ["forward", "backward"].include?(strategy) && !limit.nil?
        raise ArgumentError, "can only specify 'limit' when strategy is set to 'backward' or 'forward'"
      end

      if !value.nil?
        value = Utils.expr_to_lit_or_expr(value, str_to_lit: true)
        wrap_expr(_rbexpr.fill_null(value._rbexpr))
      else
        wrap_expr(_rbexpr.fill_null_with_strategy(strategy, limit))
      end
    end

    def fill_nan(fill_value)
      fill_value = Utils.expr_to_lit_or_expr(fill_value, str_to_lit: true)
      wrap_expr(_rbexpr.fill_nan(fill_value._rbexpr))
    end

    def reverse
      wrap_expr(_rbexpr.reverse)
    end

    def std(ddof: 1)
      wrap_expr(_rbexpr.std(ddof))
    end

    def var(ddof: 1)
      wrap_expr(_rbexpr.var(ddof))
    end

    def max
      wrap_expr(_rbexpr.max)
    end

    def min
      wrap_expr(_rbexpr.min)
    end

    def nan_max
      wrap_expr(_rbexpr.nan_max)
    end

    def nan_min
      wrap_expr(_rbexpr.nan_min)
    end

    def sum
      wrap_expr(_rbexpr.sum)
    end

    def mean
      wrap_expr(_rbexpr.mean)
    end

    def median
      wrap_expr(_rbexpr.median)
    end

    def product
      wrap_expr(_rbexpr.product)
    end

    def n_unique
      wrap_expr(_rbexpr.n_unique)
    end

    def unique(maintain_order: false)
      if maintain_order
        wrap_expr(_rbexpr.unique_stable)
      else
        wrap_expr(_rbexpr.unique)
      end
    end

    def first
      wrap_expr(_rbexpr.first)
    end

    def last
      wrap_expr(_rbexpr.last)
    end

    def over(expr)
      rbexprs = Utils.selection_to_rbexpr_list(expr)
      wrap_expr(_rbexpr.over(rbexprs))
    end

    def filter(predicate)
      wrap_expr(_rbexpr.filter(predicate._rbexpr))
    end

    def head(n = 10)
      wrap_expr(_rbexpr.head(n))
    end

    def tail(n = 10)
      wrap_expr(_rbexpr.tail(n))
    end

    def limit(n = 10)
      head(n)
    end

    def pow(exponent)
      exponent = Utils.expr_to_lit_or_expr(exponent)
      wrap_expr(_rbexpr.pow(exponent._rbexpr))
    end

    def interpolate
      wrap_expr(_rbexpr.interpolate)
    end

    def list
      wrap_expr(_rbexpr.list)
    end

    def str
      StringExpr.new(self)
    end

    private

    def wrap_expr(expr)
      Utils.wrap_expr(expr)
    end

    def _to_rbexpr(other)
      _to_expr(other)._rbexpr
    end

    def _to_expr(other)
      other.is_a?(Expr) ? other : Utils.lit(other)
    end
  end
end

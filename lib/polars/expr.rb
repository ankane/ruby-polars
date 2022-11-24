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

    def +(other)
      wrap_expr(_rbexpr + _to_rbexpr(other))
    end

    def -(other)
      wrap_expr(_rbexpr - _to_rbexpr(other))
    end

    def *(other)
      wrap_expr(_rbexpr * _to_rbexpr(other))
    end

    # def /(other)
    # end

    def %(other)
      wrap_expr(_rbexpr % _to_rbexpr(other))
    end

    def **(power)
      pow(power)
    end

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

    def sqrt
      self ** 0.5
    end

    def log10
      log(10)
    end

    def exp
      wrap_expr(_rbexpr.exp)
    end

    def alias(name)
      wrap_expr(_rbexpr._alias(name))
    end

    # TODO support symbols
    def exclude(columns)
      if columns.is_a?(String)
        columns = [columns]
        return wrap_expr(_rbexpr.exclude(columns))
      elsif !columns.is_a?(Array)
        columns = [columns]
        return wrap_expr(_rbexpr.exclude_dtype(columns))
      end

      if !columns.all? { |a| a.is_a?(String) } || !columns.all? { |a| Utils.is_polars_dtype(a) }
        raise ArgumentError, "input should be all string or all DataType"
      end

      if columns[0].is_a?(String)
        wrap_expr(_rbexpr.exclude(columns))
      else
        wrap_expr(_rbexpr.exclude_dtype(columns))
      end
    end

    def keep_name
      wrap_expr(_rbexpr.keep_name)
    end

    def prefix(prefix)
      wrap_expr(_rbexpr.prefix(prefix))
    end

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

    def is_finite
      wrap_expr(_rbexpr.is_finite)
    end

    def is_infinite
      wrap_expr(_rbexpr.is_infinite)
    end

    def is_nan
      wrap_expr(_rbexpr.is_nan)
    end

    def is_not_nan
      wrap_expr(_rbexpr.is_not_nan)
    end

    def agg_groups
      wrap_expr(_rbexpr.agg_groups)
    end

    def count
      wrap_expr(_rbexpr.count)
    end

    def len
      count
    end

    def slice(offset, length = nil)
      if !offset.is_a?(Expr)
        offset = Polars.lit(offset)
      end
      if !length.is_a?(Expr)
        length = Polars.lit(length)
      end
      wrap_expr(_rbexpr.slice(offset._rbexpr, length._rbexpr))
    end

    def append(other, upcast: true)
      other = Utils.expr_to_lit_or_expr(other)
      wrap_expr(_rbexpr.append(other._rbexpr, upcast))
    end

    def rechunk
      wrap_expr(_rbexpr.rechunk)
    end

    def drop_nulls
      wrap_expr(_rbexpr.drop_nulls)
    end

    def drop_nans
      wrap_expr(_rbexpr.drop_nans)
    end

    def cumsum(reverse: false)
      wrap_expr(_rbexpr.cumsum(reverse))
    end

    def cumprod(reverse: false)
      wrap_expr(_rbexpr.cumprod(reverse))
    end

    def cummin(reverse: false)
      wrap_expr(_rbexpr.cummin(reverse))
    end

    def cummax(reverse: false)
      wrap_expr(_rbexpr.cummax(reverse))
    end

    def cumcount(reverse: false)
      wrap_expr(_rbexpr.cumcount(reverse))
    end

    def floor
      wrap_expr(_rbexpr.floor)
    end

    def ceil
      wrap_expr(_rbexpr.ceil)
    end

    def round(decimals = 0)
      wrap_expr(_rbexpr.round(decimals))
    end

    def dot(other)
      other = Utils.expr_to_lit_or_expr(other, str_to_lit: false)
      wrap_expr(_rbexpr.dot(other._rbexpr))
    end

    def mode
      wrap_expr(_rbexpr.mode)
    end

    def cast(dtype, strict: true)
      dtype = Utils.rb_type_to_dtype(dtype)
      wrap_expr(_rbexpr.cast(dtype, strict))
    end

    def sort(reverse: false, nulls_last: false)
      wrap_expr(_rbexpr.sort_with(reverse, nulls_last))
    end

    def top_k(k: 5, reverse: false)
      wrap_expr(_rbexpr.top_k(k, reverse))
    end

    def arg_sort(reverse: false, nulls_last: false)
      wrap_expr(_rbexpr.arg_sort(reverse, nulls_last))
    end

    def arg_max
      wrap_expr(_rbexpr.arg_max)
    end

    def arg_min
      wrap_expr(_rbexpr.arg_min)
    end

    def search_sorted(element)
      element = Utils.expr_to_lit_or_expr(element, str_to_lit: false)
      wrap_expr(_rbexpr.search_sorted(element._rbexpr))
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

    # def take
    # end

    def shift(periods)
      wrap_expr(_rbexpr.shift(periods))
    end

    def shift_and_fill(periods, fill_value)
      fill_value = Utils.expr_to_lit_or_expr(fill_value, str_to_lit: true)
      wrap_expr(_rbexpr.shift_and_fill(periods, fill_value._rbexpr))
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

    def forward_fill(limit: nil)
      wrap_expr(_rbexpr.forward_fill(limit))
    end

    def backward_fill(limit: nil)
      wrap_expr(_rbexpr.backward_fill(limit))
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

    def null_count
      wrap_expr(_rbexpr.null_count)
    end

    def arg_unique
      wrap_expr(_rbexpr.arg_unique)
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

    def is_unique
      wrap_expr(_rbexpr.is_unique)
    end

    def is_first
      wrap_expr(_rbexpr.is_first)
    end

    def is_duplicated
      wrap_expr(_rbexpr.is_duplicated)
    end

    # def quantile
    # end

    def filter(predicate)
      wrap_expr(_rbexpr.filter(predicate._rbexpr))
    end

    # def where
    # end

    # def map
    # end

    # def apply
    # end

    # def flatten
    # end

    # def explode
    # end

    # def take_every
    # end

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

    # def is_in
    # end

    # def repeat_by
    # end

    # def is_between
    # end

    # def _hash
    # end

    # def reinterpret
    # end

    # def _inspect
    # end

    def interpolate
      wrap_expr(_rbexpr.interpolate)
    end

    # def rolling_min
    # end

    # def rolling_max
    # end

    # def rolling_mean
    # end

    # def rolling_sum
    # end

    # def rolling_std
    # end

    # def rolling_var
    # end

    # def rolling_median
    # end

    # def rolling_quantile
    # end

    # def rolling_apply
    # end

    # def rolling_skew
    # end

    # def abs
    # end

    # def argsort
    # end

    # def rank
    # end

    # def diff
    # end

    # def pct_change
    # end

    # def skew
    # end

    # def kurtosis
    # end

    # def clip
    # end

    # def clip_min
    # end

    # def clip_max
    # end

    # def lower_bound
    # end

    # def upper_bound
    # end

    # def sign
    # end

    # def sin
    # end

    # def cos
    # end

    # def tan
    # end

    # def arcsin
    # end

    # def arccos
    # end

    # def arctan
    # end

    # def sinh
    # end

    # def cosh
    # end

    # def tanh
    # end

    # def arcsinh
    # end

    # def arccosh
    # end

    # def arctanh
    # end

    # def reshape
    # end

    # def shuffle
    # end

    # def sample
    # end

    # def ewm_mean
    # end

    # def ewm_std
    # end

    # def ewm_var
    # end

    # def extend_constant
    # end

    # def value_counts
    # end

    # def unique_counts
    # end

    # def log
    # end

    # def entropy
    # end

    # def cumulative_eval
    # end

    # def set_sorted
    # end

    def list
      wrap_expr(_rbexpr.list)
    end

    # def shrink_dtype
    # end

    # def arr
    # end

    # def cat
    # end

    # def dt
    # end

    # def meta
    # end

    def str
      StringExpr.new(self)
    end

    # def struct
    # end

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

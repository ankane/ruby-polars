module Polars
  # Namespace for list related expressions.
  class ListExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Get the length of the arrays as `:u32`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2], "bar" => [["a", "b"], ["c"]]})
    #   df.select(Polars.col("bar").arr.lengths)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ bar │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 1   │
    #   # └─────┘
    def lengths
      Utils.wrap_expr(_rbexpr.arr_lengths)
    end

    # Sum all the lists in the array.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [[1], [2, 3]]})
    #   df.select(Polars.col("values").arr.sum)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 1      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 5      │
    #   # └────────┘
    def sum
      Utils.wrap_expr(_rbexpr.lst_sum)
    end

    def max
      Utils.wrap_expr(_rbexpr.lst_max)
    end

    def min
      Utils.wrap_expr(_rbexpr.lst_min)
    end

    def mean
      Utils.wrap_expr(_rbexpr.lst_mean)
    end

    def sort(reverse: false)
      Utils.wrap_expr(_rbexpr.lst_sort(reverse))
    end

    def reverse
      Utils.wrap_expr(_rbexpr.lst_reverse)
    end

    def unique
      Utils.wrap_expr(_rbexpr.lst_unique)
    end

    # def concat
    # end

    #
    def get(index)
      index = Utils.expr_to_lit_or_expr(index, str_to_lit: false)._rbexpr
      Utils.wrap_expr(_rbexpr.lst_get(index))
    end

    def [](item)
      get(item)
    end

    def first
      get(0)
    end

    def last
      get(-1)
    end

    def contains(item)
      Utils.wrap_expr(_rbexpr.arr_contains(Utils.expr_to_lit_or_expr(item)._rbexpr))
    end

    def join(separator)
      Utils.wrap_expr(_rbexpr.lst_join(separator))
    end

    def arg_min
      Utils.wrap_expr(_rbexpr.lst_arg_min)
    end

    def arg_max
      Utils.wrap_expr(_rbexpr.lst_arg_max)
    end

    def diff(n: 1, null_behavior: "ignore")
      Utils.wrap_expr(_rbexpr.lst_diff(n, null_behavior))
    end

    def shift(periods = 1)
      Utils.wrap_expr(_rbexpr.lst_shift(periods))
    end

    def slice(offset, length = nil)
      offset = Utils.expr_to_lit_or_expr(offset, str_to_lit: false)._rbexpr
      length = Utils.expr_to_lit_or_expr(length, str_to_lit: false)._rbexpr
      Utils.wrap_expr(_rbexpr.lst_slice(offset, length))
    end

    def head(n = 5)
      slice(0, n)
    end

    def tail(n = 5)
      offset = -Utils.expr_to_lit_or_expr(n, str_to_lit: false)
      slice(offset, n)
    end

    # def to_struct(n_field_strategy: "first_non_null", name_generator: nil)
    #   Utils.wrap_expr(_rbexpr.lst_to_struct(n_field_strategy, name_generator))
    # end

    #
    def eval(expr, parallel: false)
       Utils.wrap_expr(_rbexpr.lst_eval(expr._rbexpr, parallel))
    end
  end
end

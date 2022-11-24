module Polars
  class ListExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    def lengths
      Utils.wrap_expr(_rbexpr.arr_lengths)
    end

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

    # def contains
    # end

    # def join
    # end

    def arg_min
      Utils.wrap_expr(_rbexpr.lst_arg_min)
    end

    def arg_max
      Utils.wrap_expr(_rbexpr.lst_arg_max)
    end

    # def diff
    # end

    # def shift
    # end

    # def slice
    # end

    # def head
    # end

    # def tail
    # end

    # def to_struct
    # end

    # def eval
    # end
  end
end

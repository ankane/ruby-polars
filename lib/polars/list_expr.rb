module Polars
  class ListExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # def lengths
    # end

    # def sum
    # end

    # def max
    # end

    # def min
    # end

    # def mean
    # end

    # def sort
    # end

    # def reverse
    # end

    # def unique
    # end

    # def concat
    # end

    # def get
    # end

    # # def [](item)
    # #   get(item)
    # # end

    # def first
    # end

    # def last
    # end

    # def contains
    # end

    # def join
    # end

    # def arg_min
    # end

    # def arg_max
    # end

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

module Polars
  # Series.arr namespace.
  class ListNameSpace
    include ExprDispatch

    # @private
    def self._accessor
      "arr"
    end

    # @private
    attr_accessor :_s

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Get the length of the arrays as UInt32.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([[1, 2, 3], [5]])
    #   s.arr.lengths
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [u32]
    #   # [
    #   #         3
    #   #         1
    #   # ]
    def lengths
      super
    end

    # Sum all the arrays in the list.
    #
    # @return [Series]
    def sum
      super
    end

    # Compute the max value of the arrays in the list.
    #
    # @return [Series]
    def max
      super
    end

    # Compute the min value of the arrays in the list.
    #
    # @return [Series]
    def min
      super
    end

    # Compute the mean value of the arrays in the list.
    #
    # @return [Series]
    def mean
      super
    end

    # Sort the arrays in the list.
    #
    # @return [Series]
    def sort(reverse: false)
      super
    end

    # Reverse the arrays in the list.
    #
    # @return [Series]
    def reverse
      super
    end

    # Get the unique/distinct values in the list.
    #
    # @return [Series]
    def unique
      super
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @param other [Object]
    #   Columns to concat into a List Series
    #
    # @return [Series]
    def concat(other)
      super
    end

    # Get the value by index in the sublists.
    #
    # So index `0` would return the first item of every sublist
    # and index `-1` would return the last item of every sublist
    # if an index is out of bounds, it will return a `None`.
    #
    # @param index [Integer]
    #   Index to return per sublist
    #
    # @return [Series]
    def get(index)
      super
    end

    # Get the value by index in the sublists.
    #
    # @return [Series]
    def [](item)
      get(item)
    end

    # def join
    # end

    # def first
    # end

    # def last
    # end

    # def contains
    # end

    # def arg_min
    # end

    # def arg_max
    # end

    # Calculate the n-th discrete difference of every sublist.
    #
    # @param n [Integer]
    #   Number of slots to shift.
    # @param null_behavior ["ignore", "drop"]
    #   How to handle null values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.diff
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list]
    #   # [
    #   #         [null, 1, ... 1]
    #   #         [null, -8, -1]
    #   # ]
    def diff(n: 1, null_behavior: "ignore")
      super
    end

    # Shift values by the given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.shift
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list]
    #   # [
    #   #         [null, 1, ... 3]
    #   #         [null, 10, 2]
    #   # ]
    def shift(periods = 1)
      super
    end

    # Slice every sublist.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the list.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.slice(1, 2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list]
    #   # [
    #   #         [2, 3]
    #   #         [2, 1]
    #   # ]
    def slice(offset, length = nil)
      super
    end

    # Slice the first `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.head(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list]
    #   # [
    #   #         [1, 2]
    #   #         [10, 2]
    #   # ]
    def head(n = 5)
      super
    end

    # Slice the last `n` values of every sublist.
    #
    # @param n [Integer]
    #   Number of values to return for each sublist.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2, 3, 4], [10, 2, 1]])
    #   s.arr.tail(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [list]
    #   # [
    #   #         [3, 4]
    #   #         [2, 1]
    #   # ]
    def tail(n = 5)
      super
    end

    # def to_struct
    # end

    # def eval
    # end
  end
end

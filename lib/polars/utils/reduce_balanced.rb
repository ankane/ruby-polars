module Polars
  module Utils
    def self.reduce_balanced(function, iterable)
      values = iterable.to_a

      if values.empty?
        msg = "reduce_balanced() of empty iterable"
        raise TypeError, msg
      end

      if values.length == 1
        return values.shift
      end

      stack = [[0, values.length]]

      i = 0

      while i < stack.length
        offset, length = stack[i]
        half = -(length / -2)

        if length > 3
          stack << [offset + half, length - half]
        end

        if length > 2
          stack << [offset, half]
        end

        stack[i] = [offset, offset + half]

        i += 1
      end

      stack.reverse.each do |idx_l, idx_r|
        values[idx_l] = function.(values[idx_l], values[idx_r])
      end

      values[0]
    end
  end
end

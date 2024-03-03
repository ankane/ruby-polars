module Polars
  module Functions
    # Return an expression representing a column in a DataFrame.
    #
    # @return [Expr]
    def col(name, *more_names)
      if more_names.any?
        if Utils.strlike?(name)
          names_str = [name]
          names_str.concat(more_names)
          return Utils.wrap_expr(Plr.cols(names_str.map(&:to_s)))
        elsif Utils.is_polars_dtype(name)
          dtypes = [name]
          dtypes.concat(more_names)
          return Utils.wrap_expr(Plr.dtype_cols(dtypes))
        else
          msg = "invalid input for `col`\n\nExpected `str` or `DataType`, got #{name.class.name}."
          raise TypeError, msg
        end
      end

      if Utils.strlike?(name)
        Utils.wrap_expr(Plr.col(name.to_s))
      elsif Utils.is_polars_dtype(name)
        Utils.wrap_expr(Plr.dtype_cols([name]))
      elsif name.is_a?(::Array)
        names = Array(name)
        if names.empty?
          return Utils.wrap_expr(Plr.cols(names))
        end

        item = names[0]
        if Utils.strlike?(item)
          Utils.wrap_expr(Plr.cols(names.map(&:to_s)))
        elsif Utils.is_polars_dtype(item)
          Utils.wrap_expr(Plr.dtype_cols(names))
        else
          msg = "invalid input for `col`\n\nExpected iterable of type `str` or `DataType`, got iterable of type #{item.class.name}."
          raise TypeError, msg
        end
      else
        msg = "invalid input for `col`\n\nExpected `str` or `DataType`, got #{name.class.name}."
        raise TypeError, msg
      end
    end
  end
end

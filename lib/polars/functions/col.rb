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
          return Selector._by_name(names_str.map(&:to_s), strict: true, expand_patterns: true).as_expr
        elsif Utils.is_polars_dtype(name)
          dtypes = [name]
          dtypes.concat(more_names)
          return Selector._by_type(dtypes).as_expr
        else
          msg = "invalid input for `col`\n\nExpected `str` or `DataType`, got #{name.class.name}."
          raise TypeError, msg
        end
      end

      if Utils.strlike?(name)
        Utils.wrap_expr(Plr.col(name.to_s))
      elsif Utils.is_polars_dtype(name)
        dtypes = [name]
        Selector._by_dtype(dtypes).as_expr
      elsif name.is_a?(::Array) || name.is_a?(::Set)
        names = Array(name)
        if names.empty?
          return Utils.wrap_expr(Plr.cols(names))
        end

        item = names[0]
        if Utils.strlike?(item)
          Selector._by_name(names.map(&:to_s), strict: true, expand_patterns: true).as_expr
        elsif Utils.is_polars_dtype(item)
          Selector._by_dtype(names).as_expr
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

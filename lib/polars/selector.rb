module Polars
  # Base column selector expression/proxy.
  class Selector < Expr
    # @private
    attr_accessor :_rbselector

    # @private
    def self._from_rbselector(rbselector)
      slf = new
      slf._rbselector = rbselector
      slf._rbexpr = RbExpr.new_selector(rbselector)
      slf
    end

    def inspect
      Expr._from_rbexpr(_rbexpr).to_s
    end

    # @private
    def self._by_dtype(dtypes)
      selectors = []
      concrete_dtypes = []
      dtypes.each do |dt|
        if Utils.is_polars_dtype(dt)
          concrete_dtypes += [dt]
        else
          raise Todo
        end
      end

      dtype_selector = _from_rbselector(RbSelector.by_dtype(concrete_dtypes))

      if selectors.length == 0
        return dtype_selector
      end

      selector = selectors[0]
      selectors[1..].each do |s|
        selector = selector | s
      end
      if concrete_dtypes.length == 0
        selector
      else
        dtype_selector | selector
      end
    end

    # @private
    def self._by_name(names, strict:)
      _from_rbselector(RbSelector.by_name(names, strict))
    end

    def ~
      Selectors.all - self
    end

    def &(other)
      if Utils.is_column(other)
        colname = other.meta.output_name
        other = by_name(colname)
      end
      if Utils.is_selector(other)
        Selector._from_rbselector(
          _rbselector.intersect(other._rbselector)
        )
      else
        as_expr & other
      end
    end

    def |(other)
      if Utils.is_column(other)
        other = by_name(other.meta.output_name)
      end
      if Utils.is_selector(other)
        Selector._from_rbselector(
          _rbselector.union(other._rbselector)
        )
      else
        as_expr | other
      end
    end

    def -(other)
      if Utils.is_selector(other)
        Selector._from_rbselector(
          _rbselector.difference(other._rbselector)
        )
      else
        as_expr - other
      end
    end

    def ^(other)
      if Utils.is_column(other)
        other = by_name(other.meta.output_name)
      end
      if Utils.is_selector(other)
        Selector._from_rbselector(
          _rbselector.exclusive_or(other._rbselector)
        )
      else
        as_expr ^ other
      end
    end

    def exclude(columns, *more_columns)
      exclude_cols = []
      exclude_dtypes = []
      ((columns.is_a?(::Array) ? columns : [columns]) + more_columns).each do |item|
        if item.is_a?(::String)
          exclude_cols << item
        elsif Utils.is_polars_dtype(item)
          exclude_dtypes << item
        else
          msg = (
            "invalid input for `exclude`" +
            "\n\nExpected one or more `str` or `DataType`; found #{item.inspect} instead."
          )
          raise TypeError, msg
        end
      end

      if exclude_cols.any? && exclude_dtypes.any?
        msg = "cannot exclude by both column name and dtype; use a selector instead"
        raise TypeError, msg
      elsif exclude_dtypes.any?
        self - Selectors.by_dtype(exclude_dtypes)
      else
        self - Selectors.by_name(exclude_cols, require_all: false)
      end
    end

    def as_expr
      Expr._from_rbexpr(_rbexpr)
    end
  end
end

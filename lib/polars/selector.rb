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

    # Returns a string representing the Selector.
    #
    # @return [String]
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
    def self._by_name(names, strict:, expand_patterns:)
      _from_rbselector(RbSelector.by_name(names, strict, expand_patterns))
    end

    # Invert the selector.
    #
    # @return [Selector]
    def ~
      Selectors.all - self
    end

    # AND.
    #
    # @return [Selector]
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

    # OR.
    #
    # @return [Selector]
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

    # Difference.
    #
    # @return [Selector]
    def -(other)
      if Utils.is_selector(other)
        Selector._from_rbselector(
          _rbselector.difference(other._rbselector)
        )
      else
        as_expr - other
      end
    end

    # XOR.
    #
    # @return [Selector]
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

    # Exclude columns from a multi-column expression.
    #
    # Only works after a wildcard or regex column selection, and you cannot provide
    # both string column names *and* dtypes (you may prefer to use selectors instead).
    #
    # @param columns [Object]
    #   The name or datatype of the column(s) to exclude. Accepts regular expression
    #   input. Regular expressions should start with `^` and end with `$`.
    # @param more_columns [Array]
    #   Additional names or datatypes of columns to exclude, specified as positional
    #   arguments.
    #
    # @return [Selector]
    #
    # @example Exclude by column name(s):
    #   df = Polars::DataFrame.new(
    #     {
    #       "aa" => [1, 2, 3],
    #       "ba" => ["a", "b", nil],
    #       "cc" => [nil, 2.5, 1.5]
    #     }
    #   )
    #   df.select(Polars.cs.exclude("ba", "xx"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────┐
    #   # │ aa  ┆ cc   │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ null │
    #   # │ 2   ┆ 2.5  │
    #   # │ 3   ┆ 1.5  │
    #   # └─────┴──────┘
    #
    # @example Exclude using a column name, a selector, and a dtype:
    #   df.select(Polars.cs.exclude("aa", Polars.cs.string, Polars::UInt32))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ cc   │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # │ 2.5  │
    #   # │ 1.5  │
    #   # └──────┘
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
        self - Selector._by_name(exclude_cols, strict: false, expand_patterns: true)
      end
    end

    # Materialize the `selector` as a normal expression.
    #
    # This ensures that the operators `|`, `&`, `~` and `-`
    # are applied on the data and not on the selector sets.
    #
    # @return [Expr]
    #
    # @example Inverting the boolean selector will choose the non-boolean columns:
    #   df = Polars::DataFrame.new(
    #     {
    #       "colx" => ["aa", "bb", "cc"],
    #       "coly" => [true, false, true],
    #       "colz" => [1, 2, 3]
    #     }
    #   )
    #   df.select(~Polars.cs.boolean)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬──────┐
    #   # │ colx ┆ colz │
    #   # │ ---  ┆ ---  │
    #   # │ str  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ aa   ┆ 1    │
    #   # │ bb   ┆ 2    │
    #   # │ cc   ┆ 3    │
    #   # └──────┴──────┘
    #
    # @example To invert the *values* in the selected boolean columns, we need to materialize the selector as a standard expression instead:
    #   df.select(~Polars.cs.boolean.as_expr)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ coly  │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ false │
    #   # │ true  │
    #   # │ false │
    #   # └───────┘
    def as_expr
      Expr._from_rbexpr(_rbexpr)
    end
  end
end

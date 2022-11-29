module Polars
  # Expressions that can be used in various contexts.
  class Expr
    # @private
    attr_accessor :_rbexpr

    # @private
    def self._from_rbexpr(rbexpr)
      expr = Expr.allocate
      expr._rbexpr = rbexpr
      expr
    end

    # Returns a string representing the Expr.
    #
    # @return [String]
    def to_s
      _rbexpr.to_str
    end
    alias_method :inspect, :to_s

    # Bitwise XOR.
    #
    # @return [Expr]
    def ^(other)
      wrap_expr(_rbexpr._xor(_to_rbexpr(other)))
    end

    # Bitwise AND.
    #
    # @return [Expr]
    def &(other)
      wrap_expr(_rbexpr._and(_to_rbexpr(other)))
    end

    # Bitwise OR.
    #
    # @return [Expr]
    def |(other)
      wrap_expr(_rbexpr._or(_to_rbexpr(other)))
    end

    # Performs addition.
    #
    # @return [Expr]
    def +(other)
      wrap_expr(_rbexpr + _to_rbexpr(other))
    end

    # Performs subtraction.
    #
    # @return [Expr]
    def -(other)
      wrap_expr(_rbexpr - _to_rbexpr(other))
    end

    # Performs multiplication.
    #
    # @return [Expr]
    def *(other)
      wrap_expr(_rbexpr * _to_rbexpr(other))
    end

    # Performs division.
    #
    # @return [Expr]
    def /(other)
      wrap_expr(_rbexpr / _to_rbexpr(other))
    end

    # Performs floor division.
    #
    # @return [Expr]
    def floordiv(other)
      wrap_expr(_rbexpr.floordiv(_to_rbexpr(other)))
    end

    # Returns the modulo.
    #
    # @return [Expr]
    def %(other)
      wrap_expr(_rbexpr % _to_rbexpr(other))
    end

    # Raises to the power of exponent.
    #
    # @return [Expr]
    def **(power)
      pow(power)
    end

    # Greater than or equal.
    #
    # @return [Expr]
    def >=(other)
      wrap_expr(_rbexpr.gt_eq(_to_expr(other)._rbexpr))
    end

    # Less than or equal.
    #
    # @return [Expr]
    def <=(other)
      wrap_expr(_rbexpr.lt_eq(_to_expr(other)._rbexpr))
    end

    # Equal.
    #
    # @return [Expr]
    def ==(other)
      wrap_expr(_rbexpr.eq(_to_expr(other)._rbexpr))
    end

    # Not equal.
    #
    # @return [Expr]
    def !=(other)
      wrap_expr(_rbexpr.neq(_to_expr(other)._rbexpr))
    end

    # Less than.
    #
    # @return [Expr]
    def <(other)
      wrap_expr(_rbexpr.lt(_to_expr(other)._rbexpr))
    end

    # Greater than.
    #
    # @return [Expr]
    def >(other)
      wrap_expr(_rbexpr.gt(_to_expr(other)._rbexpr))
    end

    # Performs negation.
    #
    # @return [Expr]
    def -@
      Utils.lit(0) - self
    end

    # def to_physical
    # end

    # Check if any boolean value in a Boolean column is `true`.
    #
    # @return [Boolean]
    #
    # @example
    #   df = Polars::DataFrame.new({"TF" => [true, false], "FF" => [false, false]})
    #   df.select(Polars.all.any)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────┬───────┐
    #   # │ TF   ┆ FF    │
    #   # │ ---  ┆ ---   │
    #   # │ bool ┆ bool  │
    #   # ╞══════╪═══════╡
    #   # │ true ┆ false │
    #   # └──────┴───────┘
    def any
      wrap_expr(_rbexpr.any)
    end

    # Check if all boolean values in a Boolean column are `true`.
    #
    # This method is an expression - not to be confused with
    # `Polars.all` which is a function to select all columns.
    #
    # @return [Boolean]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"TT" => [true, true], "TF" => [true, false], "FF" => [false, false]}
    #   )
    #   df.select(Polars.col("*").all)
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────┬───────┬───────┐
    #   # │ TT   ┆ TF    ┆ FF    │
    #   # │ ---  ┆ ---   ┆ ---   │
    #   # │ bool ┆ bool  ┆ bool  │
    #   # ╞══════╪═══════╪═══════╡
    #   # │ true ┆ false ┆ false │
    #   # └──────┴───────┴───────┘
    def all
      wrap_expr(_rbexpr.all)
    end

    # Compute the square root of the elements.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [1.0, 2.0, 4.0]})
    #   df.select(Polars.col("values").sqrt)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ values   │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.414214 │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2.0      │
    #   # └──────────┘
    def sqrt
      self**0.5
    end

    # Compute the base 10 logarithm of the input array, element-wise.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [1.0, 2.0, 4.0]})
    #   df.select(Polars.col("values").log10)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────┐
    #   # │ values  │
    #   # │ ---     │
    #   # │ f64     │
    #   # ╞═════════╡
    #   # │ 0.0     │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 0.30103 │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 0.60206 │
    #   # └─────────┘
    def log10
      log(10)
    end

    # Compute the exponential, element-wise.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [1.0, 2.0, 4.0]})
    #   df.select(Polars.col("values").exp)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ values   │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 2.718282 │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 7.389056 │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 54.59815 │
    #   # └──────────┘
    def exp
      wrap_expr(_rbexpr.exp)
    end

    # Rename the output of an expression.
    #
    # @param name [String]
    #   New name.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["a", "b", nil]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("a").alias("bar"),
    #       Polars.col("b").alias("foo")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────┐
    #   # │ bar ┆ foo  │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ str  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ a    │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2   ┆ b    │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 3   ┆ null │
    #   # └─────┴──────┘
    def alias(name)
      wrap_expr(_rbexpr._alias(name))
    end

    # TODO support symbols for exclude

    # Exclude certain columns from a wildcard/regex selection.
    #
    # You may also use regexes in the exclude list. They must start with `^` and end
    # with `$`.
    #
    # @param columns [Object]
    #   Column(s) to exclude from selection.
    #   This can be:
    #
    #   - a column name, or multiple column names
    #   - a regular expression starting with `^` and ending with `$`
    #   - a dtype or multiple dtypes
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "aa" => [1, 2, 3],
    #       "ba" => ["a", "b", nil],
    #       "cc" => [nil, 2.5, 1.5]
    #     }
    #   )
    #   df.select(Polars.all.exclude("ba"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────┐
    #   # │ aa  ┆ cc   │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ null │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2   ┆ 2.5  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 3   ┆ 1.5  │
    #   # └─────┴──────┘
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

    # Keep the original root name of the expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2],
    #       "b" => [3, 4]
    #     }
    #   )
    #   df.with_columns([(Polars.col("a") * 9).alias("c").keep_name])
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 9   ┆ 3   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 18  ┆ 4   │
    #   # └─────┴─────┘
    def keep_name
      wrap_expr(_rbexpr.keep_name)
    end

    # Add a prefix to the root column name of the expression.
    #
    # @return [Expr]
    def prefix(prefix)
      wrap_expr(_rbexpr.prefix(prefix))
    end

    # Add a suffix to the root column name of the expression.
    #
    # @return [Expr]
    def suffix(suffix)
      wrap_expr(_rbexpr.suffix(suffix))
    end

    # def map_alias
    # end

    # Negate a boolean expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [true, false, false],
    #       "b" => ["a", "b", nil]
    #     }
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────┬──────┐
    #   # │ a     ┆ b    │
    #   # │ ---   ┆ ---  │
    #   # │ bool  ┆ str  │
    #   # ╞═══════╪══════╡
    #   # │ true  ┆ a    │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ false ┆ b    │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ false ┆ null │
    #   # └───────┴──────┘
    #
    # @example
    #   df.select(Polars.col("a").is_not)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ a     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ false │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ true  │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ true  │
    #   # └───────┘
    def is_not
      wrap_expr(_rbexpr.is_not)
    end

    # Returns a boolean Series indicating which values are null.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil, 1, 5],
    #       "b" => [1.0, 2.0, Float::NAN, 1.0, 5.0]
    #     }
    #   )
    #   df.with_column(Polars.all.is_null.suffix("_isnull"))
    #   # =>
    #   # shape: (5, 4)
    #   # ┌──────┬─────┬──────────┬──────────┐
    #   # │ a    ┆ b   ┆ a_isnull ┆ b_isnull │
    #   # │ ---  ┆ --- ┆ ---      ┆ ---      │
    #   # │ i64  ┆ f64 ┆ bool     ┆ bool     │
    #   # ╞══════╪═════╪══════════╪══════════╡
    #   # │ 1    ┆ 1.0 ┆ false    ┆ false    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2    ┆ 2.0 ┆ false    ┆ false    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ NaN ┆ true     ┆ false    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1    ┆ 1.0 ┆ false    ┆ false    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 5.0 ┆ false    ┆ false    │
    #   # └──────┴─────┴──────────┴──────────┘
    def is_null
      wrap_expr(_rbexpr.is_null)
    end

    # Returns a boolean Series indicating which values are not null.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil, 1, 5],
    #       "b" => [1.0, 2.0, Float::NAN, 1.0, 5.0]
    #     }
    #   )
    #   df.with_column(Polars.all.is_not_null.suffix("_not_null"))
    #   # =>
    #   # shape: (5, 4)
    #   # ┌──────┬─────┬────────────┬────────────┐
    #   # │ a    ┆ b   ┆ a_not_null ┆ b_not_null │
    #   # │ ---  ┆ --- ┆ ---        ┆ ---        │
    #   # │ i64  ┆ f64 ┆ bool       ┆ bool       │
    #   # ╞══════╪═════╪════════════╪════════════╡
    #   # │ 1    ┆ 1.0 ┆ true       ┆ true       │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2    ┆ 2.0 ┆ true       ┆ true       │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ NaN ┆ false      ┆ true       │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1    ┆ 1.0 ┆ true       ┆ true       │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 5.0 ┆ true       ┆ true       │
    #   # └──────┴─────┴────────────┴────────────┘
    def is_not_null
      wrap_expr(_rbexpr.is_not_null)
    end

    # Returns a boolean Series indicating which values are finite.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [1.0, 2],
    #       "B" => [3.0, Float::INFINITY]
    #     }
    #   )
    #   df.select(Polars.all.is_finite)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌──────┬───────┐
    #   # │ A    ┆ B     │
    #   # │ ---  ┆ ---   │
    #   # │ bool ┆ bool  │
    #   # ╞══════╪═══════╡
    #   # │ true ┆ true  │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ true ┆ false │
    #   # └──────┴───────┘
    def is_finite
      wrap_expr(_rbexpr.is_finite)
    end

    # Returns a boolean Series indicating which values are infinite.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [1.0, 2],
    #       "B" => [3.0, Float::INFINITY]
    #     }
    #   )
    #   df.select(Polars.all.is_infinite)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬───────┐
    #   # │ A     ┆ B     │
    #   # │ ---   ┆ ---   │
    #   # │ bool  ┆ bool  │
    #   # ╞═══════╪═══════╡
    #   # │ false ┆ false │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ false ┆ true  │
    #   # └───────┴───────┘
    def is_infinite
      wrap_expr(_rbexpr.is_infinite)
    end

    # Returns a boolean Series indicating which values are NaN.
    #
    # @note
    #   Floating point `NaN` (Not A Number) should not be confused
    #   with missing data represented as `nil`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil, 1, 5],
    #       "b" => [1.0, 2.0, Float::NAN, 1.0, 5.0]
    #     }
    #   )
    #   df.with_column(Polars.col(Polars::Float64).is_nan.suffix("_isnan"))
    #   # =>
    #   # shape: (5, 3)
    #   # ┌──────┬─────┬─────────┐
    #   # │ a    ┆ b   ┆ b_isnan │
    #   # │ ---  ┆ --- ┆ ---     │
    #   # │ i64  ┆ f64 ┆ bool    │
    #   # ╞══════╪═════╪═════════╡
    #   # │ 1    ┆ 1.0 ┆ false   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ 2    ┆ 2.0 ┆ false   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ NaN ┆ true    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ 1    ┆ 1.0 ┆ false   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 5.0 ┆ false   │
    #   # └──────┴─────┴─────────┘
    def is_nan
      wrap_expr(_rbexpr.is_nan)
    end

    # Returns a boolean Series indicating which values are not NaN.
    #
    # @note
    #   Floating point `NaN` (Not A Number) should not be confused
    #   with missing data represented as `nil`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil, 1, 5],
    #       "b" => [1.0, 2.0, Float::NAN, 1.0, 5.0]
    #     }
    #   )
    #   df.with_column(Polars.col(Polars::Float64).is_not_nan.suffix("_is_not_nan"))
    #   # =>
    #   # shape: (5, 3)
    #   # ┌──────┬─────┬──────────────┐
    #   # │ a    ┆ b   ┆ b_is_not_nan │
    #   # │ ---  ┆ --- ┆ ---          │
    #   # │ i64  ┆ f64 ┆ bool         │
    #   # ╞══════╪═════╪══════════════╡
    #   # │ 1    ┆ 1.0 ┆ true         │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2    ┆ 2.0 ┆ true         │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ NaN ┆ false        │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1    ┆ 1.0 ┆ true         │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 5.0 ┆ true         │
    #   # └──────┴─────┴──────────────┘
    def is_not_nan
      wrap_expr(_rbexpr.is_not_nan)
    end

    # Get the group indexes of the group by operation.
    #
    # Should be used in aggregation context only.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "group" => [
    #         "one",
    #         "one",
    #         "one",
    #         "two",
    #         "two",
    #         "two"
    #       ],
    #       "value" => [94, 95, 96, 97, 97, 99]
    #     }
    #   )
    #   df.groupby("group", maintain_order: true).agg(Polars.col("value").agg_groups)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬───────────┐
    #   # │ group ┆ value     │
    #   # │ ---   ┆ ---       │
    #   # │ str   ┆ list[u32] │
    #   # ╞═══════╪═══════════╡
    #   # │ one   ┆ [0, 1, 2] │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ two   ┆ [3, 4, 5] │
    #   # └───────┴───────────┘
    def agg_groups
      wrap_expr(_rbexpr.agg_groups)
    end

    # Count the number of values in this expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [8, 9, 10], "b" => [nil, 4, 4]})
    #   df.select(Polars.all.count)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 3   │
    #   # └─────┴─────┘
    def count
      wrap_expr(_rbexpr.count)
    end

    # Count the number of values in this expression.
    #
    # Alias for {#count}.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [8, 9, 10], "b" => [nil, 4, 4]})
    #   df.select(Polars.all.len)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 3   │
    #   # └─────┴─────┘
    def len
      count
    end

    # Get a slice of this expression.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil`, all rows starting at the offset
    #   will be selected.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [8, 9, 10, 11],
    #       "b" => [nil, 4, 4, 4]
    #     }
    #   )
    #   df.select(Polars.all.slice(1, 2))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 9   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 10  ┆ 4   │
    #   # └─────┴─────┘
    def slice(offset, length = nil)
      if !offset.is_a?(Expr)
        offset = Polars.lit(offset)
      end
      if !length.is_a?(Expr)
        length = Polars.lit(length)
      end
      wrap_expr(_rbexpr.slice(offset._rbexpr, length._rbexpr))
    end

    # Append expressions.
    #
    # This is done by adding the chunks of `other` to this `Series`.
    #
    # @param other [Expr]
    #   Expression to append.
    # @param upcast [Boolean]
    #   Cast both `Series` to the same supertype.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [8, 9, 10],
    #       "b" => [nil, 4, 4]
    #     }
    #   )
    #   df.select(Polars.all.head(1).append(Polars.all.tail(1)))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ i64  │
    #   # ╞═════╪══════╡
    #   # │ 8   ┆ null │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 10  ┆ 4    │
    #   # └─────┴──────┘
    def append(other, upcast: true)
      other = Utils.expr_to_lit_or_expr(other)
      wrap_expr(_rbexpr.append(other._rbexpr, upcast))
    end

    # Create a single chunk of memory for this Series.
    #
    # @return [Expr]
    #
    # @example Create a Series with 3 nulls, append column a then rechunk
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.repeat(nil, 3).append(Polars.col("a")).rechunk)
    #   # =>
    #   # shape: (6, 1)
    #   # ┌─────────┐
    #   # │ literal │
    #   # │ ---     │
    #   # │ i64     │
    #   # ╞═════════╡
    #   # │ null    │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ null    │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ null    │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 1       │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 1       │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 2       │
    #   # └─────────┘
    def rechunk
      wrap_expr(_rbexpr.rechunk)
    end

    # Drop null values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [8, 9, 10, 11],
    #       "b" => [nil, 4.0, 4.0, Float::NAN]
    #     }
    #   )
    #   df.select(Polars.col("b").drop_nulls)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ b   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 4.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 4.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ NaN │
    #   # └─────┘
    def drop_nulls
      wrap_expr(_rbexpr.drop_nulls)
    end

    # Drop floating point NaN values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [8, 9, 10, 11],
    #       "b" => [nil, 4.0, 4.0, Float::NAN]
    #     }
    #   )
    #   df.select(Polars.col("b").drop_nans)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ b    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # └──────┘
    def drop_nans
      wrap_expr(_rbexpr.drop_nans)
    end

    # Get an array with the cumulative sum computed at every element.
    #
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @note
    #   Dtypes in `:i8`, `:u8`, `:i16`, and `:u16` are cast to
    #   `:i64` before summing to prevent overflow issues.
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4]})
    #   df.select(
    #     [
    #       Polars.col("a").cumsum,
    #       Polars.col("a").cumsum(reverse: true).alias("a_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ a_reverse │
    #   # │ --- ┆ ---       │
    #   # │ i64 ┆ i64       │
    #   # ╞═════╪═══════════╡
    #   # │ 1   ┆ 10        │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 9         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 6   ┆ 7         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 10  ┆ 4         │
    #   # └─────┴───────────┘
    def cumsum(reverse: false)
      wrap_expr(_rbexpr.cumsum(reverse))
    end

    # Get an array with the cumulative product computed at every element.
    #
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @note
    #   Dtypes in `:i8`, `:u8`, `:i16`, and `:u16` are cast to
    #   `:i64` before summing to prevent overflow issues.
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4]})
    #   df.select(
    #     [
    #       Polars.col("a").cumprod,
    #       Polars.col("a").cumprod(reverse: true).alias("a_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ a_reverse │
    #   # │ --- ┆ ---       │
    #   # │ i64 ┆ i64       │
    #   # ╞═════╪═══════════╡
    #   # │ 1   ┆ 24        │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 24        │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 6   ┆ 12        │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 24  ┆ 4         │
    #   # └─────┴───────────┘
    def cumprod(reverse: false)
      wrap_expr(_rbexpr.cumprod(reverse))
    end

    # Get an array with the cumulative min computed at every element.
    #
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4]})
    #   df.select(
    #     [
    #       Polars.col("a").cummin,
    #       Polars.col("a").cummin(reverse: true).alias("a_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ a_reverse │
    #   # │ --- ┆ ---       │
    #   # │ i64 ┆ i64       │
    #   # ╞═════╪═══════════╡
    #   # │ 1   ┆ 1         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1   ┆ 2         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1   ┆ 3         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1   ┆ 4         │
    #   # └─────┴───────────┘
    def cummin(reverse: false)
      wrap_expr(_rbexpr.cummin(reverse))
    end

    # Get an array with the cumulative max computed at every element.
    #
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4]})
    #   df.select(
    #     [
    #       Polars.col("a").cummax,
    #       Polars.col("a").cummax(reverse: true).alias("a_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ a_reverse │
    #   # │ --- ┆ ---       │
    #   # │ i64 ┆ i64       │
    #   # ╞═════╪═══════════╡
    #   # │ 1   ┆ 4         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 4         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 4         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 4   ┆ 4         │
    #   # └─────┴───────────┘
    def cummax(reverse: false)
      wrap_expr(_rbexpr.cummax(reverse))
    end

    # Get an array with the cumulative count computed at every element.
    #
    # Counting from 0 to len
    #
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4]})
    #   df.select(
    #     [
    #       Polars.col("a").cumcount,
    #       Polars.col("a").cumcount(reverse: true).alias("a_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ a_reverse │
    #   # │ --- ┆ ---       │
    #   # │ u32 ┆ u32       │
    #   # ╞═════╪═══════════╡
    #   # │ 0   ┆ 3         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1   ┆ 2         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 1         │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 0         │
    #   # └─────┴───────────┘
    def cumcount(reverse: false)
      wrap_expr(_rbexpr.cumcount(reverse))
    end

    # Rounds down to the nearest integer value.
    #
    # Only works on floating point Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.3, 0.5, 1.0, 1.1]})
    #   df.select(Polars.col("a").floor)
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 0.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.0 │
    #   # └─────┘
    def floor
      wrap_expr(_rbexpr.floor)
    end

    # Rounds up to the nearest integer value.
    #
    # Only works on floating point Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.3, 0.5, 1.0, 1.1]})
    #   df.select(Polars.col("a").ceil)
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 2.0 │
    #   # └─────┘
    def ceil
      wrap_expr(_rbexpr.ceil)
    end

    # Round underlying floating point data by `decimals` digits.
    #
    # @param decimals [Integer]
    #   Number of decimals to round by.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.33, 0.52, 1.02, 1.17]})
    #   df.select(Polars.col("a").round(1))
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.3 │
    #   # ├╌╌╌╌╌┤
    #   # │ 0.5 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.2 │
    #   # └─────┘
    def round(decimals = 0)
      wrap_expr(_rbexpr.round(decimals))
    end

    # Compute the dot/inner product between two Expressions.
    #
    # @param other [Expr]
    #   Expression to compute dot product with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.select(Polars.col("a").dot(Polars.col("b")))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 44  │
    #   # └─────┘
    def dot(other)
      other = Utils.expr_to_lit_or_expr(other, str_to_lit: false)
      wrap_expr(_rbexpr.dot(other._rbexpr))
    end

    # Compute the most occurring value(s).
    #
    # Can return multiple Values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 1, 2, 3],
    #       "b" => [1, 1, 2, 2]
    #     }
    #   )
    #   df.select(Polars.all.mode)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 1   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1   ┆ 2   │
    #   # └─────┴─────┘
    def mode
      wrap_expr(_rbexpr.mode)
    end

    # Cast between data types.
    #
    # @param dtype [Symbol]
    #   DataType to cast to.
    # @param strict [Boolean]
    #   Throw an error if a cast could not be done.
    #   For instance, due to an overflow.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["4", "5", "6"]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       Polars.col("a").cast(:f64),
    #       Polars.col("b").cast(:i32)
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ i32 │
    #   # ╞═════╪═════╡
    #   # │ 1.0 ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2.0 ┆ 5   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3.0 ┆ 6   │
    #   # └─────┴─────┘
    def cast(dtype, strict: true)
      dtype = Utils.rb_type_to_dtype(dtype)
      wrap_expr(_rbexpr.cast(dtype, strict))
    end

    # Sort this column. In projection/ selection context the whole column is sorted.
    #
    # If used in a groupby context, the groups are sorted.
    #
    # @param reverse [Boolean]
    #   false -> order from small to large.
    #   true -> order from large to small.
    # @param nulls_last [Boolean]
    #   If true nulls are considered to be larger than any valid value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "group" => [
    #           "one",
    #           "one",
    #           "one",
    #           "two",
    #           "two",
    #           "two"
    #       ],
    #       "value" => [1, 98, 2, 3, 99, 4]
    #     }
    #   )
    #   df.select(Polars.col("value").sort)
    #   # =>
    #   # shape: (6, 1)
    #   # ┌───────┐
    #   # │ value │
    #   # │ ---   │
    #   # │ i64   │
    #   # ╞═══════╡
    #   # │ 1     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 2     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 3     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 4     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 98    │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 99    │
    #   # └───────┘
    #
    # @example
    #   df.select(Polars.col("value").sort)
    #   # =>
    #   # shape: (6, 1)
    #   # ┌───────┐
    #   # │ value │
    #   # │ ---   │
    #   # │ i64   │
    #   # ╞═══════╡
    #   # │ 1     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 2     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 3     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 4     │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 98    │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ 99    │
    #   # └───────┘
    #
    # @example
    #   df.groupby("group").agg(Polars.col("value").sort)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬────────────┐
    #   # │ group ┆ value      │
    #   # │ ---   ┆ ---        │
    #   # │ str   ┆ list[i64]  │
    #   # ╞═══════╪════════════╡
    #   # │ two   ┆ [3, 4, 99] │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ one   ┆ [1, 2, 98] │
    #   # └───────┴────────────┘
    def sort(reverse: false, nulls_last: false)
      wrap_expr(_rbexpr.sort_with(reverse, nulls_last))
    end

    # Return the `k` largest elements.
    #
    # If 'reverse: true` the smallest elements will be given.
    #
    # @param k [Integer]
    #   Number of elements to return.
    # @param reverse [Boolean]
    #   Return the smallest elements.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "value" => [1, 98, 2, 3, 99, 4]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("value").top_k.alias("top_k"),
    #       Polars.col("value").top_k(reverse: true).alias("bottom_k")
    #     ]
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌───────┬──────────┐
    #   # │ top_k ┆ bottom_k │
    #   # │ ---   ┆ ---      │
    #   # │ i64   ┆ i64      │
    #   # ╞═══════╪══════════╡
    #   # │ 99    ┆ 1        │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 98    ┆ 2        │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 4     ┆ 3        │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3     ┆ 4        │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2     ┆ 98       │
    #   # └───────┴──────────┘
    def top_k(k: 5, reverse: false)
      wrap_expr(_rbexpr.top_k(k, reverse))
    end

    # Get the index values that would sort this column.
    #
    # @param reverse [Boolean]
    #   Sort in reverse (descending) order.
    # @param nulls_last [Boolean]
    #   Place null values last instead of first.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [20, 10, 30]
    #     }
    #   )
    #   df.select(Polars.col("a").arg_sort)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 0   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # └─────┘
    def arg_sort(reverse: false, nulls_last: false)
      wrap_expr(_rbexpr.arg_sort(reverse, nulls_last))
    end

    # Get the index of the maximal value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [20, 10, 30]
    #     }
    #   )
    #   df.select(Polars.col("a").arg_max)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    def arg_max
      wrap_expr(_rbexpr.arg_max)
    end

    # Get the index of the minimal value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [20, 10, 30]
    #     }
    #   )
    #   df.select(Polars.col("a").arg_min)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # └─────┘
    def arg_min
      wrap_expr(_rbexpr.arg_min)
    end

    # Find indices where elements should be inserted to maintain order.
    #
    # @param element [Object]
    #   Expression or scalar value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "values" => [1, 2, 3, 5]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("values").search_sorted(0).alias("zero"),
    #       Polars.col("values").search_sorted(3).alias("three"),
    #       Polars.col("values").search_sorted(6).alias("six")
    #     ]
    #   )
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────┬───────┬─────┐
    #   # │ zero ┆ three ┆ six │
    #   # │ ---  ┆ ---   ┆ --- │
    #   # │ u32  ┆ u32   ┆ u32 │
    #   # ╞══════╪═══════╪═════╡
    #   # │ 0    ┆ 2     ┆ 4   │
    #   # └──────┴───────┴─────┘
    def search_sorted(element)
      element = Utils.expr_to_lit_or_expr(element, str_to_lit: false)
      wrap_expr(_rbexpr.search_sorted(element._rbexpr))
    end

    # Sort this column by the ordering of another column, or multiple other columns.
    #
    # In projection/ selection context the whole column is sorted.
    # If used in a groupby context, the groups are sorted.
    #
    # @param by [Object]
    #   The column(s) used for sorting.
    # @param reverse [Boolean]
    #   false -> order from small to large.
    #   true -> order from large to small.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "group" => [
    #         "one",
    #         "one",
    #         "one",
    #         "two",
    #         "two",
    #         "two"
    #       ],
    #       "value" => [1, 98, 2, 3, 99, 4]
    #     }
    #   )
    #   df.select(Polars.col("group").sort_by("value"))
    #   # =>
    #   # shape: (6, 1)
    #   # ┌───────┐
    #   # │ group │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │ one   │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ one   │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ two   │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ two   │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ one   │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ two   │
    #   # └───────┘
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

    # Take values by index.
    #
    # @param indices [Expr]
    #   An expression that leads to a `:u32` dtyped Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "group" => [
    #         "one",
    #         "one",
    #         "one",
    #         "two",
    #         "two",
    #         "two"
    #       ],
    #       "value" => [1, 98, 2, 3, 99, 4]
    #     }
    #   )
    #   df.groupby("group", maintain_order: true).agg(Polars.col("value").take(1))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬───────┐
    #   # │ group ┆ value │
    #   # │ ---   ┆ ---   │
    #   # │ str   ┆ i64   │
    #   # ╞═══════╪═══════╡
    #   # │ one   ┆ 98    │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ two   ┆ 99    │
    #   # └───────┴───────┘
    def take(indices)
      if indices.is_a?(Array)
        indices_lit = Polars.lit(Series.new("", indices, dtype: :u32))
      else
        indices_lit = Utils.expr_to_lit_or_expr(indices, str_to_lit: false)
      end
      wrap_expr(_rbexpr.take(indices_lit._rbexpr))
    end

    # Shift the values by a given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4]})
    #   df.select(Polars.col("foo").shift(1))
    #   # =>
    #   # shape: (4, 1)
    #   # ┌──────┐
    #   # │ foo  │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 1    │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2    │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3    │
    #   # └──────┘
    def shift(periods = 1)
      wrap_expr(_rbexpr.shift(periods))
    end

    # Shift the values by a given period and fill the resulting null values.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill nil values with the result of this expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4]})
    #   df.select(Polars.col("foo").shift_and_fill(1, "a"))
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ a   │
    #   # ├╌╌╌╌╌┤
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # └─────┘
    def shift_and_fill(periods, fill_value)
      fill_value = Utils.expr_to_lit_or_expr(fill_value, str_to_lit: true)
      wrap_expr(_rbexpr.shift_and_fill(periods, fill_value._rbexpr))
    end

    # Fill null values using the specified value or strategy.
    #
    # To interpolate over null values see interpolate.
    #
    # @param value [Object]
    #   Value used to fill null values.
    # @param strategy [nil, "forward", "backward", "min", "max", "mean", "zero", "one"]
    #   Strategy used to fill null values.
    # @param limit [Integer]
    #   Number of consecutive null values to fill when using the 'forward' or
    #   'backward' strategy.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil],
    #       "b" => [4, nil, 6]
    #     }
    #   )
    #   df.fill_null(strategy: "zero")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 0   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 0   ┆ 6   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.fill_null(99)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 99  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 99  ┆ 6   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.fill_null(strategy: "forward")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 6   │
    #   # └─────┴─────┘
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

    # Fill floating point NaN value with a fill value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.0, nil, Float::NAN],
    #       "b" => [4.0, Float::NAN, 6]
    #     }
    #   )
    #   df.fill_nan("zero")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ str  ┆ str  │
    #   # ╞══════╪══════╡
    #   # │ 1.0  ┆ 4.0  │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ null ┆ zero │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ zero ┆ 6.0  │
    #   # └──────┴──────┘
    def fill_nan(fill_value)
      fill_value = Utils.expr_to_lit_or_expr(fill_value, str_to_lit: true)
      wrap_expr(_rbexpr.fill_nan(fill_value._rbexpr))
    end

    # Fill missing values with the latest seen values.
    #
    # @param limit [Integer]
    #   The number of consecutive null values to forward fill.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil],
    #       "b" => [4, nil, 6]
    #     }
    #   )
    #   df.select(Polars.all.forward_fill)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 6   │
    #   # └─────┴─────┘
    def forward_fill(limit: nil)
      wrap_expr(_rbexpr.forward_fill(limit))
    end

    # Fill missing values with the next to be seen values.
    #
    # @param limit [Integer]
    #   The number of consecutive null values to backward fill.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil],
    #       "b" => [4, nil, 6]
    #     }
    #   )
    #   df.select(Polars.all.backward_fill)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬─────┐
    #   # │ a    ┆ b   │
    #   # │ ---  ┆ --- │
    #   # │ i64  ┆ i64 │
    #   # ╞══════╪═════╡
    #   # │ 1    ┆ 4   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2    ┆ 6   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ null ┆ 6   │
    #   # └──────┴─────┘
    def backward_fill(limit: nil)
      wrap_expr(_rbexpr.backward_fill(limit))
    end

    # Reverse the selection.
    #
    # @return [Expr]
    def reverse
      wrap_expr(_rbexpr.reverse)
    end

    # Get standard deviation.
    #
    # @param ddof [Integer]
    #   Degrees of freedom.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 0, 1]})
    #   df.select(Polars.col("a").std)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # └─────┘
    def std(ddof: 1)
      wrap_expr(_rbexpr.std(ddof))
    end

    # Get variance.
    #
    # @param ddof [Integer]
    #   Degrees of freedom.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 0, 1]})
    #   df.select(Polars.col("a").var)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # └─────┘
    def var(ddof: 1)
      wrap_expr(_rbexpr.var(ddof))
    end

    # Get maximum value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1.0, Float::NAN, 1.0]})
    #   df.select(Polars.col("a").max)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # └─────┘
    def max
      wrap_expr(_rbexpr.max)
    end

    # Get minimum value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1.0, Float::NAN, 1.0]})
    #   df.select(Polars.col("a").min)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────┐
    #   # │ a    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ -1.0 │
    #   # └──────┘
    def min
      wrap_expr(_rbexpr.min)
    end

    # Get maximum value, but propagate/poison encountered NaN values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.0, Float::NAN]})
    #   df.select(Polars.col("a").nan_max)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ NaN │
    #   # └─────┘
    def nan_max
      wrap_expr(_rbexpr.nan_max)
    end

    # Get minimum value, but propagate/poison encountered NaN values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.0, Float::NAN]})
    #   df.select(Polars.col("a").nan_min)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ NaN │
    #   # └─────┘
    def nan_min
      wrap_expr(_rbexpr.nan_min)
    end

    # Get sum value.
    #
    # @return [Expr]
    #
    # @note
    #   Dtypes in `:i8`, `:u8`, `:i16`, and `:u16` are cast to
    #   `:i64` before summing to prevent overflow issues.
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 0, 1]})
    #   df.select(Polars.col("a").sum)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 0   │
    #   # └─────┘
    def sum
      wrap_expr(_rbexpr.sum)
    end

    # Get mean value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 0, 1]})
    #   df.select(Polars.col("a").mean)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.0 │
    #   # └─────┘
    def mean
      wrap_expr(_rbexpr.mean)
    end

    # Get median value using linear interpolation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 0, 1]})
    #   df.select(Polars.col("a").median)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.0 │
    #   # └─────┘
    def median
      wrap_expr(_rbexpr.median)
    end

    # Compute the product of an expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").product)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 6   │
    #   # └─────┘
    def product
      wrap_expr(_rbexpr.product)
    end

    # Count unique values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").n_unique)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    def n_unique
      wrap_expr(_rbexpr.n_unique)
    end

    # Count null values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 1, nil],
    #       "b" => [1, 2, 3]
    #     }
    #   )
    #   df.select(Polars.all.null_count)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ u32 ┆ u32 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 0   │
    #   # └─────┴─────┘
    def null_count
      wrap_expr(_rbexpr.null_count)
    end

    # Get index of first unique value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [8, 9, 10],
    #       "b" => [nil, 4, 4]
    #     }
    #   )
    #   df.select(Polars.col("a").arg_unique)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 0   │
    #   # ├╌╌╌╌╌┤
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.col("b").arg_unique)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ b   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 0   │
    #   # ├╌╌╌╌╌┤
    #   # │ 1   │
    #   # └─────┘
    def arg_unique
      wrap_expr(_rbexpr.arg_unique)
    end

    # Get unique values of this expression.
    #
    # @param maintain_order [Boolean]
    #   Maintain order of data. This requires more work.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").unique(maintain_order: true))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # └─────┘
    def unique(maintain_order: false)
      if maintain_order
        wrap_expr(_rbexpr.unique_stable)
      else
        wrap_expr(_rbexpr.unique)
      end
    end

    # Get the first value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").first)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # └─────┘
    def first
      wrap_expr(_rbexpr.first)
    end

    # Get the last value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").last)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    def last
      wrap_expr(_rbexpr.last)
    end

    # Apply window function over a subgroup.
    #
    # This is similar to a groupby + aggregation + self join.
    # Or similar to [window functions in Postgres](https://www.postgresql.org/docs/current/tutorial-window.html).
    #
    # @param expr [Object]
    #   Column(s) to group by.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "groups" => ["g1", "g1", "g2"],
    #       "values" => [1, 2, 3]
    #     }
    #   )
    #   df.with_column(
    #     Polars.col("values").max.over("groups").alias("max_by_group")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬────────┬──────────────┐
    #   # │ groups ┆ values ┆ max_by_group │
    #   # │ ---    ┆ ---    ┆ ---          │
    #   # │ str    ┆ i64    ┆ i64          │
    #   # ╞════════╪════════╪══════════════╡
    #   # │ g1     ┆ 1      ┆ 2            │
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ g1     ┆ 2      ┆ 2            │
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ g2     ┆ 3      ┆ 3            │
    #   # └────────┴────────┴──────────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "groups" => [1, 1, 2, 2, 1, 2, 3, 3, 1],
    #       "values" => [1, 2, 3, 4, 5, 6, 7, 8, 8]
    #     }
    #   )
    #   df.lazy
    #     .select([Polars.col("groups").sum.over("groups")])
    #     .collect
    #   # =>
    #   # shape: (9, 1)
    #   # ┌────────┐
    #   # │ groups │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 4      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 4      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 6      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 6      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ ...    │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 6      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 6      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 6      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 4      │
    #   # └────────┘
    def over(expr)
      rbexprs = Utils.selection_to_rbexpr_list(expr)
      wrap_expr(_rbexpr.over(rbexprs))
    end

    # Get mask of unique values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").is_unique)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ a     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ false │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ false │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ true  │
    #   # └───────┘
    def is_unique
      wrap_expr(_rbexpr.is_unique)
    end

    # Get a mask of the first unique value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "num" => [1, 2, 3, 1, 5]
    #     }
    #   )
    #   df.with_column(Polars.col("num").is_first.alias("is_first"))
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬──────────┐
    #   # │ num ┆ is_first │
    #   # │ --- ┆ ---      │
    #   # │ i64 ┆ bool     │
    #   # ╞═════╪══════════╡
    #   # │ 1   ┆ true     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ true     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ true     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1   ┆ false    │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5   ┆ true     │
    #   # └─────┴──────────┘
    def is_first
      wrap_expr(_rbexpr.is_first)
    end

    # Get mask of duplicated values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").is_duplicated)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ a     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ true  │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ true  │
    #   # ├╌╌╌╌╌╌╌┤
    #   # │ false │
    #   # └───────┘
    def is_duplicated
      wrap_expr(_rbexpr.is_duplicated)
    end

    # Get quantile value.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0, 1, 2, 3, 4, 5]})
    #   df.select(Polars.col("a").quantile(0.3))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.col("a").quantile(0.3, interpolation: "higher"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 2.0 │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.col("a").quantile(0.3, interpolation: "lower"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.col("a").quantile(0.3, interpolation: "midpoint"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.5 │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.col("a").quantile(0.3, interpolation: "linear"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.5 │
    #   # └─────┘
    def quantile(quantile, interpolation: "nearest")
      wrap_expr(_rbexpr.quantile(quantile, interpolation))
    end

    # Filter a single column.
    #
    # Mostly useful in an aggregation context. If you want to filter on a DataFrame
    # level, use `LazyFrame#filter`.
    #
    # @param predicate [Expr]
    #   Boolean expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "group_col" => ["g1", "g1", "g2"],
    #       "b" => [1, 2, 3]
    #     }
    #   )
    #   (
    #     df.groupby("group_col").agg(
    #       [
    #         Polars.col("b").filter(Polars.col("b") < 2).sum.alias("lt"),
    #         Polars.col("b").filter(Polars.col("b") >= 2).sum.alias("gte")
    #       ]
    #     )
    #   ).sort("group_col")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────────┬──────┬─────┐
    #   # │ group_col ┆ lt   ┆ gte │
    #   # │ ---       ┆ ---  ┆ --- │
    #   # │ str       ┆ i64  ┆ i64 │
    #   # ╞═══════════╪══════╪═════╡
    #   # │ g1        ┆ 1    ┆ 2   │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ g2        ┆ null ┆ 3   │
    #   # └───────────┴──────┴─────┘
    def filter(predicate)
      wrap_expr(_rbexpr.filter(predicate._rbexpr))
    end

    # Filter a single column.
    #
    # Alias for {#filter}.
    #
    # @param predicate [Expr]
    #   Boolean expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "group_col" => ["g1", "g1", "g2"],
    #       "b" => [1, 2, 3]
    #     }
    #   )
    #   (
    #     df.groupby("group_col").agg(
    #       [
    #         Polars.col("b").where(Polars.col("b") < 2).sum.alias("lt"),
    #         Polars.col("b").where(Polars.col("b") >= 2).sum.alias("gte")
    #       ]
    #     )
    #   ).sort("group_col")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────────┬──────┬─────┐
    #   # │ group_col ┆ lt   ┆ gte │
    #   # │ ---       ┆ ---  ┆ --- │
    #   # │ str       ┆ i64  ┆ i64 │
    #   # ╞═══════════╪══════╪═════╡
    #   # │ g1        ┆ 1    ┆ 2   │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ g2        ┆ null ┆ 3   │
    #   # └───────────┴──────┴─────┘
    def where(predicate)
      filter(predicate)
    end

    # def map
    # end

    # def apply
    # end

    # Explode a list or utf8 Series. This means that every item is expanded to a new
    # row.
    #
    # Alias for {#explode}.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["hello", "world"]})
    #   df.select(Polars.col("foo").flatten)
    #   # =>
    #   # shape: (10, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ h   │
    #   # ├╌╌╌╌╌┤
    #   # │ e   │
    #   # ├╌╌╌╌╌┤
    #   # │ l   │
    #   # ├╌╌╌╌╌┤
    #   # │ l   │
    #   # ├╌╌╌╌╌┤
    #   # │ ... │
    #   # ├╌╌╌╌╌┤
    #   # │ o   │
    #   # ├╌╌╌╌╌┤
    #   # │ r   │
    #   # ├╌╌╌╌╌┤
    #   # │ l   │
    #   # ├╌╌╌╌╌┤
    #   # │ d   │
    #   # └─────┘
    def flatten
      wrap_expr(_rbexpr.explode)
    end

    # Explode a list or utf8 Series.
    #
    # This means that every item is expanded to a new row.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"b" => [[1, 2, 3], [4, 5, 6]]})
    #   df.select(Polars.col("b").explode)
    #   # =>
    #   # shape: (6, 1)
    #   # ┌─────┐
    #   # │ b   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # ├╌╌╌╌╌┤
    #   # │ 4   │
    #   # ├╌╌╌╌╌┤
    #   # │ 5   │
    #   # ├╌╌╌╌╌┤
    #   # │ 6   │
    #   # └─────┘
    def explode
      wrap_expr(_rbexpr.explode)
    end

    # Take every nth value in the Series and return as a new Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5, 6, 7, 8, 9]})
    #   df.select(Polars.col("foo").take_every(3))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 4   │
    #   # ├╌╌╌╌╌┤
    #   # │ 7   │
    #   # └─────┘
    def take_every(n)
      wrap_expr(_rbexpr.take_every(n))
    end

    # Get the first `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5, 6, 7]})
    #   df.head(3)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # └─────┘
    def head(n = 10)
      wrap_expr(_rbexpr.head(n))
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5, 6, 7]})
    #   df.tail(3)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 5   │
    #   # ├╌╌╌╌╌┤
    #   # │ 6   │
    #   # ├╌╌╌╌╌┤
    #   # │ 7   │
    #   # └─────┘
    def tail(n = 10)
      wrap_expr(_rbexpr.tail(n))
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Expr]
    def limit(n = 10)
      head(n)
    end

    # Raise expression to the power of exponent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4]})
    #   df.select(Polars.col("foo").pow(3))
    #   # =>
    #   # shape: (4, 1)
    #   # ┌──────┐
    #   # │ foo  │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ 1.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 8.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 27.0 │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 64.0 │
    #   # └──────┘
    def pow(exponent)
      exponent = Utils.expr_to_lit_or_expr(exponent)
      wrap_expr(_rbexpr.pow(exponent._rbexpr))
    end

    # def is_in
    # end

    # Repeat the elements in this Series as specified in the given expression.
    #
    # The repeated elements are expanded into a `List`.
    #
    # @param by [Object]
    #   Numeric column that determines how often the values will be repeated.
    #   The column will be coerced to UInt32. Give this dtype to make the coercion a
    #   no-op.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "n" => [1, 2, 3]
    #     }
    #   )
    #   df.select(Polars.col("a").repeat_by("n"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────────┐
    #   # │ a               │
    #   # │ ---             │
    #   # │ list[str]       │
    #   # ╞═════════════════╡
    #   # │ ["x"]           │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ ["y", "y"]      │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ ["z", "z", "z"] │
    #   # └─────────────────┘
    def repeat_by(by)
      by = Utils.expr_to_lit_or_expr(by, str_to_lit: false)
      wrap_expr(_rbexpr.repeat_by(by._rbexpr))
    end

    # def is_between
    # end

    # def _hash
    # end

    # Reinterpret the underlying bits as a signed/unsigned integer.
    #
    # This operation is only allowed for 64bit integers. For lower bits integers,
    # you can safely use that cast operation.
    #
    # @param signed [Boolean]
    #   If true, reinterpret as `:i64`. Otherwise, reinterpret as `:u64`.
    #
    # @return [Expr]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 1, 2], dtype: :u64)
    #   df = Polars::DataFrame.new([s])
    #   df.select(
    #     [
    #       Polars.col("a").reinterpret(signed: true).alias("reinterpreted"),
    #       Polars.col("a").alias("original")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────────┬──────────┐
    #   # │ reinterpreted ┆ original │
    #   # │ ---           ┆ ---      │
    #   # │ i64           ┆ u64      │
    #   # ╞═══════════════╪══════════╡
    #   # │ 1             ┆ 1        │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1             ┆ 1        │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2             ┆ 2        │
    #   # └───────────────┴──────────┘
    def reinterpret(signed: false)
      wrap_expr(_rbexpr.reinterpret(signed))
    end

    # def _inspect
    # end

    # Fill nulls with linear interpolation over missing values.
    #
    # Can also be used to regrid data to a new grid - see examples below.
    #
    # @return [Expr]
    #
    # @example Fill nulls with linear interpolation
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, nil, 3],
    #       "b" => [1.0, Float::NAN, 3.0]
    #     }
    #   )
    #   df.select(Polars.all.interpolate)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 1.0 │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ NaN │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 3.0 │
    #   # └─────┴─────┘
    def interpolate
      wrap_expr(_rbexpr.interpolate)
    end

    # Apply a rolling min (moving min) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_min(2)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────┐
    #   # │ A    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 1.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 5.0  │
    #   # └──────┘
    def rolling_min(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_min(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Apply a rolling max (moving max) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_max(2)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────┐
    #   # │ A    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 5.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 6.0  │
    #   # └──────┘
    def rolling_max(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_max(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Apply a rolling mean (moving mean) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 8.0, 6.0, 2.0, 16.0, 10.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_mean(2)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────┐
    #   # │ A    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.5  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 7.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 9.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 13.0 │
    #   # └──────┘
    def rolling_mean(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_mean(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Apply a rolling sum (moving sum) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_sum(2)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────┐
    #   # │ A    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 5.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 7.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 9.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 11.0 │
    #   # └──────┘
    def rolling_sum(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_sum(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Compute a rolling standard deviation.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 6.0, 8.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_std(3)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────────┐
    #   # │ A        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ null     │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null     │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.527525 │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2.0      │
    #   # └──────────┘
    def rolling_std(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_std(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Compute a rolling variance.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 6.0, 8.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_var(3)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────────┐
    #   # │ A        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ null     │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null     │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2.333333 │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 4.0      │
    #   # └──────────┘
    def rolling_var(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_var(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Compute a rolling median.
    #
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 6.0, 8.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_median(3)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────┐
    #   # │ A    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 6.0  │
    #   # └──────┘
    def rolling_median(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_median(
          window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # Compute a rolling quantile.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    # @param window_size [Integer]
    #   The length of the window. Can be a fixed integer size, or a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 day)
    #   - 1w    (1 week)
    #   - 1mo   (1 calendar month)
    #   - 1y    (1 calendar year)
    #   - 1i    (1 index count)
    #
    #   If a timedelta or the dynamic string language is used, the `by`
    #   and `closed` arguments must also be set.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    # @param by [String]
    #   If the `window_size` is temporal for instance `"5h"` or `"3s`, you must
    #   set the column that will be used to determine the windows. This column must
    #   be of dtype `{Date, Datetime}`
    # @param closed ["left", "right", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `groupby_rolling` this method can cache the window size
    #   computation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 3.0, 4.0, 6.0, 8.0]})
    #   df.select(
    #     [
    #       Polars.col("A").rolling_quantile(0.33, window_size: 3)
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 1)
    #   # ┌──────┐
    #   # │ A    │
    #   # │ ---  │
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 1.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 3.0  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 4.0  │
    #   # └──────┘
    def rolling_quantile(
      quantile,
      interpolation: "nearest",
      window_size: 2,
      weights: nil,
      min_periods: nil,
      center: false,
      by: nil,
      closed: "left"
    )
      window_size, min_periods = _prepare_rolling_window_args(
        window_size, min_periods
      )
      wrap_expr(
        _rbexpr.rolling_quantile(
          quantile, interpolation, window_size, weights, min_periods, center, by, closed
        )
      )
    end

    # def rolling_apply
    # end

    # Compute a rolling skew.
    #
    # @param window_size [Integer]
    #   Integer size of the rolling window.
    # @param bias [Boolean]
    #   If false, the calculations are corrected for statistical bias.
    #
    # @return [Expr]
    def rolling_skew(window_size, bias: true)
      wrap_expr(_rbexpr.rolling_skew(window_size, bias))
    end

    # Compute absolute values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [-1.0, 0.0, 1.0, 2.0]
    #     }
    #   )
    #   df.select(Polars.col("A").abs)
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────┐
    #   # │ A   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 0.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 2.0 │
    #   # └─────┘
    def abs
      wrap_expr(_rbexpr.abs)
    end

    # Get the index values that would sort this column.
    #
    # Alias for {#arg_sort}.
    #
    # @param reverse [Boolean]
    #   Sort in reverse (descending) order.
    # @param nulls_last [Boolean]
    #   Place null values last instead of first.
    #
    # @return [expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [20, 10, 30]
    #     }
    #   )
    #   df.select(Polars.col("a").argsort)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 0   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # └─────┘
    def argsort(reverse: false, nulls_last: false)
      arg_sort(reverse: reverse, nulls_last: nulls_last)
    end

    # Assign ranks to data, dealing with ties appropriately.
    #
    # @param method ["average", "min", "max", "dense", "ordinal", "random"]
    #   The method used to assign ranks to tied elements.
    #   The following methods are available:
    #
    #   - 'average' : The average of the ranks that would have been assigned to
    #     all the tied values is assigned to each value.
    #   - 'min' : The minimum of the ranks that would have been assigned to all
    #     the tied values is assigned to each value. (This is also referred to
    #     as "competition" ranking.)
    #   - 'max' : The maximum of the ranks that would have been assigned to all
    #     the tied values is assigned to each value.
    #   - 'dense' : Like 'min', but the rank of the next highest element is
    #     assigned the rank immediately after those assigned to the tied
    #     elements.
    #   - 'ordinal' : All values are given a distinct rank, corresponding to
    #     the order that the values occur in the Series.
    #   - 'random' : Like 'ordinal', but the rank for ties is not dependent
    #     on the order that the values occur in the Series.
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Expr]
    #
    # @example The 'average' method:
    #   df = Polars::DataFrame.new({"a" => [3, 6, 1, 1, 6]})
    #   df.select(Polars.col("a").rank)
    #   # =>
    #   # shape: (5, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f32 │
    #   # ╞═════╡
    #   # │ 3.0 │
    #   # ├╌╌╌╌╌┤
    #   # │ 4.5 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.5 │
    #   # ├╌╌╌╌╌┤
    #   # │ 1.5 │
    #   # ├╌╌╌╌╌┤
    #   # │ 4.5 │
    #   # └─────┘
    #
    # @example The 'ordinal' method:
    #   df = Polars::DataFrame.new({"a" => [3, 6, 1, 1, 6]})
    #   df.select(Polars.col("a").rank(method: "ordinal"))
    #   # =>
    #   # shape: (5, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 3   │
    #   # ├╌╌╌╌╌┤
    #   # │ 4   │
    #   # ├╌╌╌╌╌┤
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 5   │
    #   # └─────┘
    def rank(method: "average", reverse: false)
      wrap_expr(_rbexpr.rank(method, reverse))
    end

    # Calculate the n-th discrete difference.
    #
    # @param n [Integer]
    #   Number of slots to shift.
    # @param null_behavior ["ignore", "drop"]
    #   How to handle null values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [20, 10, 30]
    #     }
    #   )
    #   df.select(Polars.col("a").diff)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ a    │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ null │
    #   # ├╌╌╌╌╌╌┤
    #   # │ -10  │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 20   │
    #   # └──────┘
    def diff(n: 1, null_behavior: "ignore")
      wrap_expr(_rbexpr.diff(n, null_behavior))
    end

    # Computes percentage change between values.
    #
    # Percentage change (as fraction) between current element and most-recent
    # non-null element at least `n` period(s) before the current element.
    #
    # Computes the change from the previous row by default.
    #
    # @param n [Integer]
    #   Periods to shift for forming percent change.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [10, 11, 12, nil, 12]
    #     }
    #   )
    #   df.with_column(Polars.col("a").pct_change.alias("pct_change"))
    #   # =>
    #   # shape: (5, 2)
    #   # ┌──────┬────────────┐
    #   # │ a    ┆ pct_change │
    #   # │ ---  ┆ ---        │
    #   # │ i64  ┆ f64        │
    #   # ╞══════╪════════════╡
    #   # │ 10   ┆ null       │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 11   ┆ 0.1        │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 12   ┆ 0.090909   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ 0.0        │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 12   ┆ 0.0        │
    #   # └──────┴────────────┘
    def pct_change(n: 1)
      wrap_expr(_rbexpr.pct_change(n))
    end

    # Compute the sample skewness of a data set.
    #
    # For normally distributed data, the skewness should be about zero. For
    # unimodal continuous distributions, a skewness value greater than zero means
    # that there is more weight in the right tail of the distribution. The
    # function `skewtest` can be used to determine if the skewness value
    # is close enough to zero, statistically speaking.
    #
    # @param bias [Boolean]
    #   If false, the calculations are corrected for statistical bias.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 2, 1]})
    #   df.select(Polars.col("a").skew)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.343622 │
    #   # └──────────┘
    def skew(bias: true)
      wrap_expr(_rbexpr.skew(bias))
    end

    # Compute the kurtosis (Fisher or Pearson) of a dataset.
    #
    # Kurtosis is the fourth central moment divided by the square of the
    # variance. If Fisher's definition is used, then 3.0 is subtracted from
    # the result to give 0.0 for a normal distribution.
    # If bias is False then the kurtosis is calculated using k statistics to
    # eliminate bias coming from biased moment estimators
    #
    # @param fisher [Boolean]
    #   If true, Fisher's definition is used (normal ==> 0.0). If false,
    #   Pearson's definition is used (normal ==> 3.0).
    # @param bias [Boolean]
    #   If false, the calculations are corrected for statistical bias.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 2, 1]})
    #   df.select(Polars.col("a").kurtosis)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ f64       │
    #   # ╞═══════════╡
    #   # │ -1.153061 │
    #   # └───────────┘
    def kurtosis(fisher: true, bias: true)
      wrap_expr(_rbexpr.kurtosis(fisher, bias))
    end

    # Clip (limit) the values in an array to a `min` and `max` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See `when` for more information.
    #
    # @param min_val [Numeric]
    #   Minimum value.
    # @param max_val [Numeric]
    #   Maximum value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [-50, 5, nil, 50]})
    #   df.with_column(Polars.col("foo").clip(1, 10).alias("foo_clipped"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬─────────────┐
    #   # │ foo  ┆ foo_clipped │
    #   # │ ---  ┆ ---         │
    #   # │ i64  ┆ i64         │
    #   # ╞══════╪═════════════╡
    #   # │ -50  ┆ 1           │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 5           │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ null        │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 50   ┆ 10          │
    #   # └──────┴─────────────┘
    def clip(min_val, max_val)
      wrap_expr(_rbexpr.clip(min_val, max_val))
    end

    # Clip (limit) the values in an array to a `min` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See `when` for more information.
    #
    # @param min_val [Numeric]
    #   Minimum value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [-50, 5, nil, 50]})
    #   df.with_column(Polars.col("foo").clip_min(0).alias("foo_clipped"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬─────────────┐
    #   # │ foo  ┆ foo_clipped │
    #   # │ ---  ┆ ---         │
    #   # │ i64  ┆ i64         │
    #   # ╞══════╪═════════════╡
    #   # │ -50  ┆ 0           │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 5           │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ null        │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 50   ┆ 50          │
    #   # └──────┴─────────────┘
    def clip_min(min_val)
      wrap_expr(_rbexpr.clip_min(min_val))
    end

    # Clip (limit) the values in an array to a `max` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See `when` for more information.
    #
    # @param max_val [Numeric]
    #   Maximum value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [-50, 5, nil, 50]})
    #   df.with_column(Polars.col("foo").clip_max(0).alias("foo_clipped"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬─────────────┐
    #   # │ foo  ┆ foo_clipped │
    #   # │ ---  ┆ ---         │
    #   # │ i64  ┆ i64         │
    #   # ╞══════╪═════════════╡
    #   # │ -50  ┆ -50         │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5    ┆ 0           │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null ┆ null        │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 50   ┆ 0           │
    #   # └──────┴─────────────┘
    def clip_max(max_val)
      wrap_expr(_rbexpr.clip_max(max_val))
    end

    # Calculate the lower bound.
    #
    # Returns a unit Series with the lowest value possible for the dtype of this
    # expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 2, 1]})
    #   df.select(Polars.col("a").lower_bound)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────────────────┐
    #   # │ a                    │
    #   # │ ---                  │
    #   # │ i64                  │
    #   # ╞══════════════════════╡
    #   # │ -9223372036854775808 │
    #   # └──────────────────────┘
    def lower_bound
      wrap_expr(_rbexpr.lower_bound)
    end

    # Calculate the upper bound.
    #
    # Returns a unit Series with the highest value possible for the dtype of this
    # expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 2, 1]})
    #   df.select(Polars.col("a").upper_bound)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────────────────────┐
    #   # │ a                   │
    #   # │ ---                 │
    #   # │ i64                 │
    #   # ╞═════════════════════╡
    #   # │ 9223372036854775807 │
    #   # └─────────────────────┘
    def upper_bound
      wrap_expr(_rbexpr.upper_bound)
    end

    # Compute the element-wise indication of the sign.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-9.0, -0.0, 0.0, 4.0, nil]})
    #   df.select(Polars.col("a").sign)
    #   # =>
    #   # shape: (5, 1)
    #   # ┌──────┐
    #   # │ a    │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ -1   │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 0    │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 0    │
    #   # ├╌╌╌╌╌╌┤
    #   # │ 1    │
    #   # ├╌╌╌╌╌╌┤
    #   # │ null │
    #   # └──────┘
    def sign
      wrap_expr(_rbexpr.sign)
    end

    # Compute the element-wise value for the sine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.0]})
    #   df.select(Polars.col("a").sin)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.0 │
    #   # └─────┘
    def sin
      wrap_expr(_rbexpr.sin)
    end

    # Compute the element-wise value for the cosine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.0]})
    #   df.select(Polars.col("a").cos)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 1.0 │
    #   # └─────┘
    def cos
      wrap_expr(_rbexpr.cos)
    end

    # Compute the element-wise value for the tangent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").tan)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.557408 │
    #   # └──────────┘
    def tan
      wrap_expr(_rbexpr.tan)
    end

    # Compute the element-wise value for the inverse sine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").arcsin)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.570796 │
    #   # └──────────┘
    def arcsin
      wrap_expr(_rbexpr.arcsin)
    end

    # Compute the element-wise value for the inverse cosine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [0.0]})
    #   df.select(Polars.col("a").arccos)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.570796 │
    #   # └──────────┘
    def arccos
      wrap_expr(_rbexpr.arccos)
    end

    # Compute the element-wise value for the inverse tangent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").arctan)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.785398 │
    #   # └──────────┘
    def arctan
      wrap_expr(_rbexpr.arctan)
    end

    # Compute the element-wise value for the hyperbolic sine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").sinh)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.175201 │
    #   # └──────────┘
    def sinh
      wrap_expr(_rbexpr.sinh)
    end

    # Compute the element-wise value for the hyperbolic cosine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").cosh)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.543081 │
    #   # └──────────┘
    def cosh
      wrap_expr(_rbexpr.cosh)
    end

    # Compute the element-wise value for the hyperbolic tangent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").tanh)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.761594 │
    #   # └──────────┘
    def tanh
      wrap_expr(_rbexpr.tanh)
    end

    # Compute the element-wise value for the inverse hyperbolic sine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").arcsinh)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.881374 │
    #   # └──────────┘
    def arcsinh
      wrap_expr(_rbexpr.arcsinh)
    end

    # Compute the element-wise value for the inverse hyperbolic cosine.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").arccosh)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 0.0 │
    #   # └─────┘
    def arccosh
      wrap_expr(_rbexpr.arccosh)
    end

    # Compute the element-wise value for the inverse hyperbolic tangent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1.0]})
    #   df.select(Polars.col("a").arctanh)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ inf │
    #   # └─────┘
    def arctanh
      wrap_expr(_rbexpr.arctanh)
    end

    # Reshape this Expr to a flat Series or a Series of Lists.
    #
    # @param dims [Array]
    #   Tuple of the dimension sizes. If a -1 is used in any of the dimensions, that
    #   dimension is inferred.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5, 6, 7, 8, 9]})
    #   df.select(Polars.col("foo").reshape([3, 3]))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────────┐
    #   # │ foo       │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2, 3] │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ [4, 5, 6] │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ [7, 8, 9] │
    #   # └───────────┘
    def reshape(dims)
      wrap_expr(_rbexpr.reshape(dims))
    end

    # Shuffle the contents of this expr.
    #
    # @param seed [Integer]
    #   Seed for the random number generator. If set to None (default), a random
    #   seed is generated using the `random` module.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").shuffle(seed: 1))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # └─────┘
    def shuffle(seed: nil)
      if seed.nil?
        seed = rand(10000)
      end
      wrap_expr(_rbexpr.shuffle(seed))
    end

    # def sample
    # end

    # Exponentially-weighted moving average.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").ewm_mean(com: 1))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.666667 │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2.428571 │
    #   # └──────────┘
    def ewm_mean(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      min_periods: 1
    )
      alpha = _prepare_alpha(com, span, half_life, alpha)
      wrap_expr(_rbexpr.ewm_mean(alpha, adjust, min_periods))
    end

    # def ewm_std
    # end

    # def ewm_var
    # end

    # Extend the Series with given number of values.
    #
    # @param value [Object]
    #   The value to extend the Series with. This value may be nil to fill with
    #   nulls.
    # @param n [Integer]
    #   The number of values to extend.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [1, 2, 3]})
    #   df.select(Polars.col("values").extend_constant(99, 2))
    #   # =>
    #   # shape: (5, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 1      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 2      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 3      │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 99     │
    #   # ├╌╌╌╌╌╌╌╌┤
    #   # │ 99     │
    #   # └────────┘
    def extend_constant(value, n)
      wrap_expr(_rbexpr.extend_constant(value, n))
    end

    # Count all unique values and create a struct mapping value to count.
    #
    # @param multithreaded [Boolean]
    #   Better to turn this off in the aggregation context, as it can lead to
    #   contention.
    # @param sort [Boolean]
    #   Ensure the output is sorted from most values to least.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "id" => ["a", "b", "b", "c", "c", "c"]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("id").value_counts(sort: true),
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────────┐
    #   # │ id        │
    #   # │ ---       │
    #   # │ struct[2] │
    #   # ╞═══════════╡
    #   # │ {"c",3}   │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ {"b",2}   │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ {"a",1}   │
    #   # └───────────┘
    def value_counts(multithreaded: false, sort: false)
      wrap_expr(_rbexpr.value_counts(multithreaded, sort))
    end

    # Return a count of the unique values in the order of appearance.
    #
    # This method differs from `value_counts` in that it does not return the
    # values, only the counts and might be faster
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "id" => ["a", "b", "b", "c", "c", "c"]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("id").unique_counts
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ id  │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # └─────┘
    def unique_counts
      wrap_expr(_rbexpr.unique_counts)
    end

    # Compute the logarithm to a given base.
    #
    # @param base [Float]
    #   Given base, defaults to `e`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").log(2))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 1.584963 │
    #   # └──────────┘
    def log(base = Math::E)
      wrap_expr(_rbexpr.log(base))
    end

    # Computes the entropy.
    #
    # Uses the formula `-sum(pk * log(pk)` where `pk` are discrete probabilities.
    #
    # @param base [Float]
    #   Given base, defaults to `e`.
    # @param normalize [Boolean]
    #   Normalize pk if it doesn't sum to 1.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").entropy(base: 2))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 1.459148 │
    #   # └──────────┘
    #
    # @example
    #   df.select(Polars.col("a").entropy(base: 2, normalize: false))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌───────────┐
    #   # │ a         │
    #   # │ ---       │
    #   # │ f64       │
    #   # ╞═══════════╡
    #   # │ -6.754888 │
    #   # └───────────┘
    def entropy(base: 2, normalize: true)
      wrap_expr(_rbexpr.entropy(base, normalize))
    end

    # def cumulative_eval
    # end

    # def set_sorted
    # end

    # Aggregate to list.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => [4, 5, 6]
    #     }
    #   )
    #   df.select(Polars.all.list)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌───────────┬───────────┐
    #   # │ a         ┆ b         │
    #   # │ ---       ┆ ---       │
    #   # │ list[i64] ┆ list[i64] │
    #   # ╞═══════════╪═══════════╡
    #   # │ [1, 2, 3] ┆ [4, 5, 6] │
    #   # └───────────┴───────────┘
    def list
      wrap_expr(_rbexpr.list)
    end

    # Shrink numeric columns to the minimal required datatype.
    #
    # Shrink to the dtype needed to fit the extrema of this `Series`.
    # This can be used to reduce memory pressure.
    #
    # @return [Expr]
    #
    # @example
    #   Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => [1, 2, 2 << 32],
    #       "c" => [-1, 2, 1 << 30],
    #       "d" => [-112, 2, 112],
    #       "e" => [-112, 2, 129],
    #       "f" => ["a", "b", "c"],
    #       "g" => [0.1, 1.32, 0.12],
    #       "h" => [true, nil, false]
    #     }
    #   ).select(Polars.all.shrink_dtype)
    #   # =>
    #   # shape: (3, 8)
    #   # ┌─────┬────────────┬────────────┬──────┬──────┬─────┬──────┬───────┐
    #   # │ a   ┆ b          ┆ c          ┆ d    ┆ e    ┆ f   ┆ g    ┆ h     │
    #   # │ --- ┆ ---        ┆ ---        ┆ ---  ┆ ---  ┆ --- ┆ ---  ┆ ---   │
    #   # │ i8  ┆ i64        ┆ i32        ┆ i8   ┆ i16  ┆ str ┆ f32  ┆ bool  │
    #   # ╞═════╪════════════╪════════════╪══════╪══════╪═════╪══════╪═══════╡
    #   # │ 1   ┆ 1          ┆ -1         ┆ -112 ┆ -112 ┆ a   ┆ 0.1  ┆ true  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 2          ┆ 2          ┆ 2    ┆ 2    ┆ b   ┆ 1.32 ┆ null  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 8589934592 ┆ 1073741824 ┆ 112  ┆ 129  ┆ c   ┆ 0.12 ┆ false │
    #   # └─────┴────────────┴────────────┴──────┴──────┴─────┴──────┴───────┘
    def shrink_dtype
      wrap_expr(_rbexpr.shrink_dtype)
    end

    # Create an object namespace of all list related methods.
    #
    # @return [ListExpr]
    def arr
      ListExpr.new(self)
    end

    # Create an object namespace of all categorical related methods.
    #
    # @return [CatExpr]
    def cat
      CatExpr.new(self)
    end

    # Create an object namespace of all datetime related methods.
    #
    # @return [DateTimeExpr]
    def dt
      DateTimeExpr.new(self)
    end

    # Create an object namespace of all meta related expression methods.
    #
    # @return [MetaExpr]
    def meta
      MetaExpr.new(self)
    end

    # Create an object namespace of all string related methods.
    #
    # @return [StringExpr]
    def str
      StringExpr.new(self)
    end

    # Create an object namespace of all struct related methods.
    #
    # @return [StructExpr]
    def struct
      StructExpr.new(self)
    end

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

    def _prepare_alpha(com, span, half_life, alpha)
      if [com, span, half_life, alpha].count { |v| !v.nil? } > 1
        raise ArgumentError, "Parameters 'com', 'span', 'half_life', and 'alpha' are mutually exclusive"
      end

      if !com.nil?
        if com < 0.0
          raise ArgumentError, "Require 'com' >= 0 (found #{com})"
        end
        alpha = 1.0 / (1.0 + com)

      elsif !span.nil?
        if span < 1.0
          raise ArgumentError, "Require 'span' >= 1 (found #{span})"
        end
        alpha = 2.0 / (span + 1.0)

      elsif !half_life.nil?
        if half_life <= 0.0
          raise ArgumentError, "Require 'half_life' > 0 (found #{half_life})"
        end
        alpha = 1.0 - Math.exp(-Math.log(2.0) / half_life)

      elsif alpha.nil?
        raise ArgumentError, "One of 'com', 'span', 'half_life', or 'alpha' must be set"

      elsif alpha <= 0 || alpha > 1
        raise ArgumentError, "Require 0 < 'alpha' <= 1 (found #{alpha})"
      end

      alpha
    end

    def _prepare_rolling_window_args(window_size, min_periods)
      if window_size.is_a?(Integer)
        if min_periods.nil?
          min_periods = window_size
        end
        window_size = "#{window_size}i"
      end
      if min_periods.nil?
        min_periods = 1
      end
      [window_size, min_periods]
    end
  end
end

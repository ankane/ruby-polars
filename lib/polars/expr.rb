module Polars
  # Expressions that can be used in various contexts.
  class Expr
    # @private
    NO_DEFAULT = Object.new

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
      _from_rbexpr(_rbexpr._xor(_to_rbexpr(other)))
    end

    # Bitwise AND.
    #
    # @return [Expr]
    def &(other)
      _from_rbexpr(_rbexpr._and(_to_rbexpr(other)))
    end

    # Bitwise OR.
    #
    # @return [Expr]
    def |(other)
      _from_rbexpr(_rbexpr._or(_to_rbexpr(other)))
    end

    # Performs addition.
    #
    # @return [Expr]
    def +(other)
      _from_rbexpr(_rbexpr + _to_rbexpr(other))
    end

    # Performs subtraction.
    #
    # @return [Expr]
    def -(other)
      _from_rbexpr(_rbexpr - _to_rbexpr(other))
    end

    # Performs multiplication.
    #
    # @return [Expr]
    def *(other)
      _from_rbexpr(_rbexpr * _to_rbexpr(other))
    end

    # Performs division.
    #
    # @return [Expr]
    def /(other)
      _from_rbexpr(_rbexpr / _to_rbexpr(other))
    end

    # Returns the modulo.
    #
    # @return [Expr]
    def %(other)
      _from_rbexpr(_rbexpr % _to_rbexpr(other))
    end

    # Raises to the power of exponent.
    #
    # @return [Expr]
    def **(power)
      exponent = Utils.parse_into_expression(power)
      _from_rbexpr(_rbexpr.pow(exponent))
    end

    # Greater than or equal.
    #
    # @return [Expr]
    def >=(other)
      _from_rbexpr(_rbexpr.gt_eq(_to_expr(other)._rbexpr))
    end

    # Less than or equal.
    #
    # @return [Expr]
    def <=(other)
      _from_rbexpr(_rbexpr.lt_eq(_to_expr(other)._rbexpr))
    end

    # Equal.
    #
    # @return [Expr]
    def ==(other)
      _from_rbexpr(_rbexpr.eq(_to_expr(other)._rbexpr))
    end

    # Not equal.
    #
    # @return [Expr]
    def !=(other)
      _from_rbexpr(_rbexpr.neq(_to_expr(other)._rbexpr))
    end

    # Less than.
    #
    # @return [Expr]
    def <(other)
      _from_rbexpr(_rbexpr.lt(_to_expr(other)._rbexpr))
    end

    # Greater than.
    #
    # @return [Expr]
    def >(other)
      _from_rbexpr(_rbexpr.gt(_to_expr(other)._rbexpr))
    end

    # Performs boolean not.
    #
    # @return [Expr]
    def !
      is_not
    end

    # Performs negation.
    #
    # @return [Expr]
    def -@
      _from_rbexpr(_rbexpr.neg)
    end

    # Cast to physical representation of the logical dtype.
    #
    # - `:date` -> `:i32`
    # - `:datetime` -> `:i64`
    # - `:time` -> `:i64`
    # - `:duration` -> `:i64`
    # - `:cat` -> `:u32`
    # - Other data types will be left unchanged.
    #
    # @return [Expr]
    #
    # @example
    #   Polars::DataFrame.new({"vals" => ["a", "x", nil, "a"]}).with_columns(
    #     [
    #       Polars.col("vals").cast(:cat),
    #       Polars.col("vals")
    #         .cast(:cat)
    #         .to_physical
    #         .alias("vals_physical")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬───────────────┐
    #   # │ vals ┆ vals_physical │
    #   # │ ---  ┆ ---           │
    #   # │ cat  ┆ u32           │
    #   # ╞══════╪═══════════════╡
    #   # │ a    ┆ 0             │
    #   # │ x    ┆ 1             │
    #   # │ null ┆ null          │
    #   # │ a    ┆ 0             │
    #   # └──────┴───────────────┘
    def to_physical
      _from_rbexpr(_rbexpr.to_physical)
    end

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
    def any(drop_nulls: true)
      _from_rbexpr(_rbexpr.any(drop_nulls))
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
    def all(drop_nulls: true)
      _from_rbexpr(_rbexpr.all(drop_nulls))
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
    #   # │ 1.414214 │
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
    #   # │ 0.30103 │
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
    #   # │ 7.389056 │
    #   # │ 54.59815 │
    #   # └──────────┘
    def exp
      _from_rbexpr(_rbexpr.exp)
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
    #   # │ 2   ┆ b    │
    #   # │ 3   ┆ null │
    #   # └─────┴──────┘
    def alias(name)
      _from_rbexpr(_rbexpr._alias(name))
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
    #   # │ 2   ┆ 2.5  │
    #   # │ 3   ┆ 1.5  │
    #   # └─────┴──────┘
    def exclude(columns)
      if columns.is_a?(::String)
        columns = [columns]
        return _from_rbexpr(_rbexpr.exclude(columns))
      elsif !columns.is_a?(::Array)
        columns = [columns]
        return _from_rbexpr(_rbexpr.exclude_dtype(columns))
      end

      if !columns.all? { |a| a.is_a?(::String) } || !columns.all? { |a| Utils.is_polars_dtype(a) }
        raise ArgumentError, "input should be all string or all DataType"
      end

      if columns[0].is_a?(::String)
        _from_rbexpr(_rbexpr.exclude(columns))
      else
        _from_rbexpr(_rbexpr.exclude_dtype(columns))
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
    #   # │ 18  ┆ 4   │
    #   # └─────┴─────┘
    def keep_name
      name.keep
    end

    # Add a prefix to the root column name of the expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.all.reverse.name.prefix("reverse_"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬───────────┬───────────┐
    #   # │ a   ┆ b   ┆ reverse_a ┆ reverse_b │
    #   # │ --- ┆ --- ┆ ---       ┆ ---       │
    #   # │ i64 ┆ str ┆ i64       ┆ str       │
    #   # ╞═════╪═════╪═══════════╪═══════════╡
    #   # │ 1   ┆ x   ┆ 3         ┆ z         │
    #   # │ 2   ┆ y   ┆ 2         ┆ y         │
    #   # │ 3   ┆ z   ┆ 1         ┆ x         │
    #   # └─────┴─────┴───────────┴───────────┘
    def prefix(prefix)
      name.prefix(prefix)
    end

    # Add a suffix to the root column name of the expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.all.reverse.name.suffix("_reverse"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬───────────┬───────────┐
    #   # │ a   ┆ b   ┆ a_reverse ┆ b_reverse │
    #   # │ --- ┆ --- ┆ ---       ┆ ---       │
    #   # │ i64 ┆ str ┆ i64       ┆ str       │
    #   # ╞═════╪═════╪═══════════╪═══════════╡
    #   # │ 1   ┆ x   ┆ 3         ┆ z         │
    #   # │ 2   ┆ y   ┆ 2         ┆ y         │
    #   # │ 3   ┆ z   ┆ 1         ┆ x         │
    #   # └─────┴─────┴───────────┴───────────┘
    def suffix(suffix)
      name.suffix(suffix)
    end

    # Rename the output of an expression by mapping a function over the root name.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [1, 2],
    #       "B" => [3, 4]
    #     }
    #   )
    #   df.select(
    #     Polars.all.reverse.map_alias { |colName| colName + "_reverse" }
    #   )
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────────┬───────────┐
    #   # │ A_reverse ┆ B_reverse │
    #   # │ ---       ┆ ---       │
    #   # │ i64       ┆ i64       │
    #   # ╞═══════════╪═══════════╡
    #   # │ 2         ┆ 4         │
    #   # │ 1         ┆ 3         │
    #   # └───────────┴───────────┘
    def map_alias(&f)
      name.map(&f)
    end

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
    #   # │ false ┆ b    │
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
    #   # │ true  │
    #   # │ true  │
    #   # └───────┘
    def is_not
      _from_rbexpr(_rbexpr.not_)
    end
    alias_method :not_, :is_not

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
    #   # │ 2    ┆ 2.0 ┆ false    ┆ false    │
    #   # │ null ┆ NaN ┆ true     ┆ false    │
    #   # │ 1    ┆ 1.0 ┆ false    ┆ false    │
    #   # │ 5    ┆ 5.0 ┆ false    ┆ false    │
    #   # └──────┴─────┴──────────┴──────────┘
    def is_null
      _from_rbexpr(_rbexpr.is_null)
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
    #   # │ 2    ┆ 2.0 ┆ true       ┆ true       │
    #   # │ null ┆ NaN ┆ false      ┆ true       │
    #   # │ 1    ┆ 1.0 ┆ true       ┆ true       │
    #   # │ 5    ┆ 5.0 ┆ true       ┆ true       │
    #   # └──────┴─────┴────────────┴────────────┘
    def is_not_null
      _from_rbexpr(_rbexpr.is_not_null)
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
    #   # │ true ┆ false │
    #   # └──────┴───────┘
    def is_finite
      _from_rbexpr(_rbexpr.is_finite)
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
    #   # │ false ┆ true  │
    #   # └───────┴───────┘
    def is_infinite
      _from_rbexpr(_rbexpr.is_infinite)
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
    #   # │ 2    ┆ 2.0 ┆ false   │
    #   # │ null ┆ NaN ┆ true    │
    #   # │ 1    ┆ 1.0 ┆ false   │
    #   # │ 5    ┆ 5.0 ┆ false   │
    #   # └──────┴─────┴─────────┘
    def is_nan
      _from_rbexpr(_rbexpr.is_nan)
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
    #   # │ 2    ┆ 2.0 ┆ true         │
    #   # │ null ┆ NaN ┆ false        │
    #   # │ 1    ┆ 1.0 ┆ true         │
    #   # │ 5    ┆ 5.0 ┆ true         │
    #   # └──────┴─────┴──────────────┘
    def is_not_nan
      _from_rbexpr(_rbexpr.is_not_nan)
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
    #   df.group_by("group", maintain_order: true).agg(Polars.col("value").agg_groups)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬───────────┐
    #   # │ group ┆ value     │
    #   # │ ---   ┆ ---       │
    #   # │ str   ┆ list[u32] │
    #   # ╞═══════╪═══════════╡
    #   # │ one   ┆ [0, 1, 2] │
    #   # │ two   ┆ [3, 4, 5] │
    #   # └───────┴───────────┘
    def agg_groups
      _from_rbexpr(_rbexpr.agg_groups)
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
    #   # │ 3   ┆ 2   │
    #   # └─────┴─────┘
    def count
      _from_rbexpr(_rbexpr.count)
    end

    # Count the number of values in this expression.
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
      _from_rbexpr(_rbexpr.len)
    end
    alias_method :length, :len

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
    #   # │ 10  ┆ 4   │
    #   # └─────┴─────┘
    def slice(offset, length = nil)
      if !offset.is_a?(Expr)
        offset = Polars.lit(offset)
      end
      if !length.is_a?(Expr)
        length = Polars.lit(length)
      end
      _from_rbexpr(_rbexpr.slice(offset._rbexpr, length._rbexpr))
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
    #   # │ 10  ┆ 4    │
    #   # └─────┴──────┘
    def append(other, upcast: true)
      other = Utils.parse_into_expression(other)
      _from_rbexpr(_rbexpr.append(other, upcast))
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
    #   # ┌────────┐
    #   # │ repeat │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ null   │
    #   # │ null   │
    #   # │ null   │
    #   # │ 1      │
    #   # │ 1      │
    #   # │ 2      │
    #   # └────────┘
    def rechunk
      _from_rbexpr(_rbexpr.rechunk)
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
    #   # │ 4.0 │
    #   # │ NaN │
    #   # └─────┘
    def drop_nulls
      _from_rbexpr(_rbexpr.drop_nulls)
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
    #   # │ 4.0  │
    #   # │ 4.0  │
    #   # └──────┘
    def drop_nans
      _from_rbexpr(_rbexpr.drop_nans)
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
    #       Polars.col("a").cum_sum,
    #       Polars.col("a").cum_sum(reverse: true).alias("a_reverse")
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
    #   # │ 3   ┆ 9         │
    #   # │ 6   ┆ 7         │
    #   # │ 10  ┆ 4         │
    #   # └─────┴───────────┘
    def cum_sum(reverse: false)
      _from_rbexpr(_rbexpr.cum_sum(reverse))
    end
    alias_method :cumsum, :cum_sum

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
    #       Polars.col("a").cum_prod,
    #       Polars.col("a").cum_prod(reverse: true).alias("a_reverse")
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
    #   # │ 2   ┆ 24        │
    #   # │ 6   ┆ 12        │
    #   # │ 24  ┆ 4         │
    #   # └─────┴───────────┘
    def cum_prod(reverse: false)
      _from_rbexpr(_rbexpr.cum_prod(reverse))
    end
    alias_method :cumprod, :cum_prod

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
    #       Polars.col("a").cum_min,
    #       Polars.col("a").cum_min(reverse: true).alias("a_reverse")
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
    #   # │ 1   ┆ 2         │
    #   # │ 1   ┆ 3         │
    #   # │ 1   ┆ 4         │
    #   # └─────┴───────────┘
    def cum_min(reverse: false)
      _from_rbexpr(_rbexpr.cum_min(reverse))
    end
    alias_method :cummin, :cum_min

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
    #       Polars.col("a").cum_max,
    #       Polars.col("a").cum_max(reverse: true).alias("a_reverse")
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
    #   # │ 2   ┆ 4         │
    #   # │ 3   ┆ 4         │
    #   # │ 4   ┆ 4         │
    #   # └─────┴───────────┘
    def cum_max(reverse: false)
      _from_rbexpr(_rbexpr.cum_max(reverse))
    end
    alias_method :cummax, :cum_max

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
    #   df = Polars::DataFrame.new({"a" => ["x", "k", nil, "d"]})
    #   df.with_columns(
    #     [
    #       Polars.col("a").cum_count.alias("cum_count"),
    #       Polars.col("a").cum_count(reverse: true).alias("cum_count_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬───────────┬───────────────────┐
    #   # │ a    ┆ cum_count ┆ cum_count_reverse │
    #   # │ ---  ┆ ---       ┆ ---               │
    #   # │ str  ┆ u32       ┆ u32               │
    #   # ╞══════╪═══════════╪═══════════════════╡
    #   # │ x    ┆ 1         ┆ 3                 │
    #   # │ k    ┆ 2         ┆ 2                 │
    #   # │ null ┆ 2         ┆ 1                 │
    #   # │ d    ┆ 3         ┆ 1                 │
    #   # └──────┴───────────┴───────────────────┘
    def cum_count(reverse: false)
      _from_rbexpr(_rbexpr.cum_count(reverse))
    end
    alias_method :cumcount, :cum_count

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
    #   # │ 0.0 │
    #   # │ 1.0 │
    #   # │ 1.0 │
    #   # └─────┘
    def floor
      _from_rbexpr(_rbexpr.floor)
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
    #   # │ 1.0 │
    #   # │ 1.0 │
    #   # │ 2.0 │
    #   # └─────┘
    def ceil
      _from_rbexpr(_rbexpr.ceil)
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
    #   # │ 0.5 │
    #   # │ 1.0 │
    #   # │ 1.2 │
    #   # └─────┘
    def round(decimals = 0)
      _from_rbexpr(_rbexpr.round(decimals))
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
      other = Utils.parse_into_expression(other, str_as_lit: false)
      _from_rbexpr(_rbexpr.dot(other))
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
    #   df.select(Polars.all.mode.first)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 1   │
    #   # │ 1   ┆ 2   │
    #   # └─────┴─────┘
    def mode
      _from_rbexpr(_rbexpr.mode)
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
    #   # │ 2.0 ┆ 5   │
    #   # │ 3.0 ┆ 6   │
    #   # └─────┴─────┘
    def cast(dtype, strict: true)
      dtype = Utils.rb_type_to_dtype(dtype)
      _from_rbexpr(_rbexpr.cast(dtype, strict))
    end

    # Sort this column. In projection/ selection context the whole column is sorted.
    #
    # If used in a group by context, the groups are sorted.
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
    #   df.select(Polars.col("value").sort)
    #   # =>
    #   # shape: (6, 1)
    #   # ┌───────┐
    #   # │ value │
    #   # │ ---   │
    #   # │ i64   │
    #   # ╞═══════╡
    #   # │ 1     │
    #   # │ 2     │
    #   # │ 3     │
    #   # │ 4     │
    #   # │ 98    │
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
    #   # │ 2     │
    #   # │ 3     │
    #   # │ 4     │
    #   # │ 98    │
    #   # │ 99    │
    #   # └───────┘
    #
    # @example
    #   df.group_by("group").agg(Polars.col("value").sort)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬────────────┐
    #   # │ group ┆ value      │
    #   # │ ---   ┆ ---        │
    #   # │ str   ┆ list[i64]  │
    #   # ╞═══════╪════════════╡
    #   # │ two   ┆ [3, 4, 99] │
    #   # │ one   ┆ [1, 2, 98] │
    #   # └───────┴────────────┘
    def sort(reverse: false, nulls_last: false)
      _from_rbexpr(_rbexpr.sort_with(reverse, nulls_last))
    end

    # Return the `k` largest elements.
    #
    # If 'reverse: true` the smallest elements will be given.
    #
    # @param k [Integer]
    #   Number of elements to return.
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
    #       Polars.col("value").bottom_k.alias("bottom_k")
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
    #   # │ 98    ┆ 2        │
    #   # │ 4     ┆ 3        │
    #   # │ 3     ┆ 4        │
    #   # │ 2     ┆ 98       │
    #   # └───────┴──────────┘
    def top_k(k: 5)
      k = Utils.parse_into_expression(k)
      _from_rbexpr(_rbexpr.top_k(k))
    end

    # Return the `k` smallest elements.
    #
    # If 'reverse: true` the smallest elements will be given.
    #
    # @param k [Integer]
    #   Number of elements to return.
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
    #       Polars.col("value").bottom_k.alias("bottom_k")
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
    #   # │ 98    ┆ 2        │
    #   # │ 4     ┆ 3        │
    #   # │ 3     ┆ 4        │
    #   # │ 2     ┆ 98       │
    #   # └───────┴──────────┘
    def bottom_k(k: 5)
      k = Utils.parse_into_expression(k)
      _from_rbexpr(_rbexpr.bottom_k(k))
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
    #   # │ 0   │
    #   # │ 2   │
    #   # └─────┘
    def arg_sort(reverse: false, nulls_last: false)
      _from_rbexpr(_rbexpr.arg_sort(reverse, nulls_last))
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
      _from_rbexpr(_rbexpr.arg_max)
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
      _from_rbexpr(_rbexpr.arg_min)
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
    def search_sorted(element, side: "any")
      element = Utils.parse_into_expression(element, str_as_lit: false)
      _from_rbexpr(_rbexpr.search_sorted(element, side))
    end

    # Sort this column by the ordering of another column, or multiple other columns.
    #
    # In projection/ selection context the whole column is sorted.
    # If used in a group by context, the groups are sorted.
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
    #   # │ one   │
    #   # │ two   │
    #   # │ two   │
    #   # │ one   │
    #   # │ two   │
    #   # └───────┘
    def sort_by(by, *more_by, reverse: false, nulls_last: false, multithreaded: true, maintain_order: false)
      by = Utils.parse_into_list_of_expressions(by, *more_by)
      reverse = Utils.extend_bool(reverse, by.length, "reverse", "by")
      nulls_last = Utils.extend_bool(nulls_last, by.length, "nulls_last", "by")
      _from_rbexpr(
        _rbexpr.sort_by(
          by, reverse, nulls_last, multithreaded, maintain_order
        )
      )
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
    #   df.group_by("group", maintain_order: true).agg(Polars.col("value").take([2, 1]))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬───────────┐
    #   # │ group ┆ value     │
    #   # │ ---   ┆ ---       │
    #   # │ str   ┆ list[i64] │
    #   # ╞═══════╪═══════════╡
    #   # │ one   ┆ [2, 98]   │
    #   # │ two   ┆ [4, 99]   │
    #   # └───────┴───────────┘
    def gather(indices)
      if indices.is_a?(::Array)
        indices_lit = Polars.lit(Series.new("", indices, dtype: :u32))._rbexpr
      else
        indices_lit = Utils.parse_into_expression(indices, str_as_lit: false)
      end
      _from_rbexpr(_rbexpr.gather(indices_lit))
    end
    alias_method :take, :gather

    # Return a single value by index.
    #
    # @param index [Object]
    #   An expression that leads to a UInt32 index.
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
    #   df.group_by("group", maintain_order: true).agg(Polars.col("value").get(1))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌───────┬───────┐
    #   # │ group ┆ value │
    #   # │ ---   ┆ ---   │
    #   # │ str   ┆ i64   │
    #   # ╞═══════╪═══════╡
    #   # │ one   ┆ 98    │
    #   # │ two   ┆ 99    │
    #   # └───────┴───────┘
    def get(index)
      index_lit = Utils.parse_into_expression(index)
      _from_rbexpr(_rbexpr.get(index_lit))
    end

    # Shift the values by a given period.
    #
    # @param n [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill the resulting null values with this value.
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
    #   # │ 1    │
    #   # │ 2    │
    #   # │ 3    │
    #   # └──────┘
    def shift(n = 1, fill_value: nil)
      if !fill_value.nil?
        fill_value = Utils.parse_into_expression(fill_value, str_as_lit: true)
      end
      n = Utils.parse_into_expression(n)
      _from_rbexpr(_rbexpr.shift(n, fill_value))
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
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    def shift_and_fill(periods, fill_value)
      shift(periods, fill_value: fill_value)
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
    #   # │ 2   ┆ 0   │
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
    #   # │ 2   ┆ 99  │
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
    #   # │ 2   ┆ 4   │
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
        value = Utils.parse_into_expression(value, str_as_lit: true)
        _from_rbexpr(_rbexpr.fill_null(value))
      else
        _from_rbexpr(_rbexpr.fill_null_with_strategy(strategy, limit))
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
    #   # │ null ┆ zero │
    #   # │ zero ┆ 6.0  │
    #   # └──────┴──────┘
    def fill_nan(fill_value)
      fill_value = Utils.parse_into_expression(fill_value, str_as_lit: true)
      _from_rbexpr(_rbexpr.fill_nan(fill_value))
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
    #   # │ 2   ┆ 4   │
    #   # │ 2   ┆ 6   │
    #   # └─────┴─────┘
    def forward_fill(limit: nil)
      _from_rbexpr(_rbexpr.forward_fill(limit))
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
    #   # │ 2    ┆ 6   │
    #   # │ null ┆ 6   │
    #   # └──────┴─────┘
    def backward_fill(limit: nil)
      _from_rbexpr(_rbexpr.backward_fill(limit))
    end

    # Reverse the selection.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [1, 2, 3, 4, 5],
    #       "fruits" => ["banana", "banana", "apple", "apple", "banana"],
    #       "B" => [5, 4, 3, 2, 1],
    #       "cars" => ["beetle", "audi", "beetle", "beetle", "beetle"]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.all,
    #       Polars.all.reverse.name.suffix("_reverse")
    #     ]
    #   )
    #   # =>
    #   # shape: (5, 8)
    #   # ┌─────┬────────┬─────┬────────┬───────────┬────────────────┬───────────┬──────────────┐
    #   # │ A   ┆ fruits ┆ B   ┆ cars   ┆ A_reverse ┆ fruits_reverse ┆ B_reverse ┆ cars_reverse │
    #   # │ --- ┆ ---    ┆ --- ┆ ---    ┆ ---       ┆ ---            ┆ ---       ┆ ---          │
    #   # │ i64 ┆ str    ┆ i64 ┆ str    ┆ i64       ┆ str            ┆ i64       ┆ str          │
    #   # ╞═════╪════════╪═════╪════════╪═══════════╪════════════════╪═══════════╪══════════════╡
    #   # │ 1   ┆ banana ┆ 5   ┆ beetle ┆ 5         ┆ banana         ┆ 1         ┆ beetle       │
    #   # │ 2   ┆ banana ┆ 4   ┆ audi   ┆ 4         ┆ apple          ┆ 2         ┆ beetle       │
    #   # │ 3   ┆ apple  ┆ 3   ┆ beetle ┆ 3         ┆ apple          ┆ 3         ┆ beetle       │
    #   # │ 4   ┆ apple  ┆ 2   ┆ beetle ┆ 2         ┆ banana         ┆ 4         ┆ audi         │
    #   # │ 5   ┆ banana ┆ 1   ┆ beetle ┆ 1         ┆ banana         ┆ 5         ┆ beetle       │
    #   # └─────┴────────┴─────┴────────┴───────────┴────────────────┴───────────┴──────────────┘
    def reverse
      _from_rbexpr(_rbexpr.reverse)
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
      _from_rbexpr(_rbexpr.std(ddof))
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
      _from_rbexpr(_rbexpr.var(ddof))
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
      _from_rbexpr(_rbexpr.max)
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
      _from_rbexpr(_rbexpr.min)
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
      _from_rbexpr(_rbexpr.nan_max)
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
      _from_rbexpr(_rbexpr.nan_min)
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
      _from_rbexpr(_rbexpr.sum)
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
      _from_rbexpr(_rbexpr.mean)
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
      _from_rbexpr(_rbexpr.median)
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
      _from_rbexpr(_rbexpr.product)
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
      _from_rbexpr(_rbexpr.n_unique)
    end

    # Approx count unique values.
    #
    # This is done using the HyperLogLog++ algorithm for cardinality estimation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 1, 2]})
    #   df.select(Polars.col("a").approx_n_unique)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ u32 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    def approx_n_unique
      _from_rbexpr(_rbexpr.approx_n_unique)
    end
    alias_method :approx_unique, :approx_n_unique

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
      _from_rbexpr(_rbexpr.null_count)
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
    #   # │ 1   │
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
    #   # │ 1   │
    #   # └─────┘
    def arg_unique
      _from_rbexpr(_rbexpr.arg_unique)
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
    #   # │ 2   │
    #   # └─────┘
    def unique(maintain_order: false)
      if maintain_order
        _from_rbexpr(_rbexpr.unique_stable)
      else
        _from_rbexpr(_rbexpr.unique)
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
      _from_rbexpr(_rbexpr.first)
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
      _from_rbexpr(_rbexpr.last)
    end

    # Apply window function over a subgroup.
    #
    # This is similar to a group by + aggregation + self join.
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
    #   # │ g1     ┆ 2      ┆ 2            │
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
    #   # │ 4      │
    #   # │ 6      │
    #   # │ 6      │
    #   # │ 4      │
    #   # │ 6      │
    #   # │ 6      │
    #   # │ 6      │
    #   # │ 4      │
    #   # └────────┘
    def over(expr)
      rbexprs = Utils.parse_into_list_of_expressions(expr)
      _from_rbexpr(_rbexpr.over(rbexprs))
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
    #   # │ false │
    #   # │ true  │
    #   # └───────┘
    def is_unique
      _from_rbexpr(_rbexpr.is_unique)
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
    #   # │ 2   ┆ true     │
    #   # │ 3   ┆ true     │
    #   # │ 1   ┆ false    │
    #   # │ 5   ┆ true     │
    #   # └─────┴──────────┘
    def is_first_distinct
      _from_rbexpr(_rbexpr.is_first_distinct)
    end
    alias_method :is_first, :is_first_distinct

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
    #   # │ true  │
    #   # │ false │
    #   # └───────┘
    def is_duplicated
      _from_rbexpr(_rbexpr.is_duplicated)
    end

    # Get a boolean mask of the local maximum peaks.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4, 5]})
    #   df.select(Polars.col("a").peak_max)
    #   # =>
    #   # shape: (5, 1)
    #   # ┌───────┐
    #   # │ a     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ false │
    #   # │ false │
    #   # │ false │
    #   # │ false │
    #   # │ true  │
    #   # └───────┘
    def peak_max
      _from_rbexpr(_rbexpr.peak_max)
    end

    # Get a boolean mask of the local minimum peaks.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [4, 1, 3, 2, 5]})
    #   df.select(Polars.col("a").peak_min)
    #   # =>
    #   # shape: (5, 1)
    #   # ┌───────┐
    #   # │ a     │
    #   # │ ---   │
    #   # │ bool  │
    #   # ╞═══════╡
    #   # │ false │
    #   # │ true  │
    #   # │ false │
    #   # │ true  │
    #   # │ false │
    #   # └───────┘
    def peak_min
      _from_rbexpr(_rbexpr.peak_min)
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
    #   # │ 2.0 │
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
      quantile = Utils.parse_into_expression(quantile, str_as_lit: false)
      _from_rbexpr(_rbexpr.quantile(quantile, interpolation))
    end

    # Bin continuous values into discrete categories.
    #
    # @param breaks [Array]
    #   List of unique cut points.
    # @param labels [Array]
    #   Names of the categories. The number of labels must be equal to the number
    #   of cut points plus one.
    # @param left_closed [Boolean]
    #   Set the intervals to be left-closed instead of right-closed.
    # @param include_breaks [Boolean]
    #   Include a column with the right endpoint of the bin each observation falls
    #   in. This will change the data type of the output from a
    #   `Categorical` to a `Struct`.
    #
    # @return [Expr]
    #
    # @example Divide a column into three categories.
    #   df = Polars::DataFrame.new({"foo" => [-2, -1, 0, 1, 2]})
    #   df.with_columns(
    #     Polars.col("foo").cut([-1, 1], labels: ["a", "b", "c"]).alias("cut")
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ cut │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ cat │
    #   # ╞═════╪═════╡
    #   # │ -2  ┆ a   │
    #   # │ -1  ┆ a   │
    #   # │ 0   ┆ b   │
    #   # │ 1   ┆ b   │
    #   # │ 2   ┆ c   │
    #   # └─────┴─────┘
    #
    # @example Add both the category and the breakpoint.
    #   df.with_columns(
    #     Polars.col("foo").cut([-1, 1], include_breaks: true).alias("cut")
    #   ).unnest("cut")
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬────────────┬────────────┐
    #   # │ foo ┆ breakpoint ┆ category   │
    #   # │ --- ┆ ---        ┆ ---        │
    #   # │ i64 ┆ f64        ┆ cat        │
    #   # ╞═════╪════════════╪════════════╡
    #   # │ -2  ┆ -1.0       ┆ (-inf, -1] │
    #   # │ -1  ┆ -1.0       ┆ (-inf, -1] │
    #   # │ 0   ┆ 1.0        ┆ (-1, 1]    │
    #   # │ 1   ┆ 1.0        ┆ (-1, 1]    │
    #   # │ 2   ┆ inf        ┆ (1, inf]   │
    #   # └─────┴────────────┴────────────┘
    def cut(breaks, labels: nil, left_closed: false, include_breaks: false)
      _from_rbexpr(_rbexpr.cut(breaks, labels, left_closed, include_breaks))
    end

    # Bin continuous values into discrete categories based on their quantiles.
    #
    # @param quantiles [Array]
    #   Either a list of quantile probabilities between 0 and 1 or a positive
    #   integer determining the number of bins with uniform probability.
    # @param labels [Array]
    #   Names of the categories. The number of labels must be equal to the number
    #   of categories.
    # @param left_closed [Boolean]
    #   Set the intervals to be left-closed instead of right-closed.
    # @param allow_duplicates [Boolean]
    #   If set to `true`, duplicates in the resulting quantiles are dropped,
    #   rather than raising a `DuplicateError`. This can happen even with unique
    #   probabilities, depending on the data.
    # @param include_breaks [Boolean]
    #   Include a column with the right endpoint of the bin each observation falls
    #   in. This will change the data type of the output from a
    #   `Categorical` to a `Struct`.
    #
    # @return [Expr]
    #
    # @example Divide a column into three categories according to pre-defined quantile probabilities.
    #   df = Polars::DataFrame.new({"foo" => [-2, -1, 0, 1, 2]})
    #   df.with_columns(
    #     Polars.col("foo").qcut([0.25, 0.75], labels: ["a", "b", "c"]).alias("qcut")
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬──────┐
    #   # │ foo ┆ qcut │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ cat  │
    #   # ╞═════╪══════╡
    #   # │ -2  ┆ a    │
    #   # │ -1  ┆ a    │
    #   # │ 0   ┆ b    │
    #   # │ 1   ┆ b    │
    #   # │ 2   ┆ c    │
    #   # └─────┴──────┘
    #
    # @example Divide a column into two categories using uniform quantile probabilities.
    #   df.with_columns(
    #     Polars.col("foo")
    #       .qcut(2, labels: ["low", "high"], left_closed: true)
    #       .alias("qcut")
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬──────┐
    #   # │ foo ┆ qcut │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ cat  │
    #   # ╞═════╪══════╡
    #   # │ -2  ┆ low  │
    #   # │ -1  ┆ low  │
    #   # │ 0   ┆ high │
    #   # │ 1   ┆ high │
    #   # │ 2   ┆ high │
    #   # └─────┴──────┘
    #
    # @example Add both the category and the breakpoint.
    #   df.with_columns(
    #     Polars.col("foo").qcut([0.25, 0.75], include_breaks: true).alias("qcut")
    #   ).unnest("qcut")
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬────────────┬────────────┐
    #   # │ foo ┆ breakpoint ┆ category   │
    #   # │ --- ┆ ---        ┆ ---        │
    #   # │ i64 ┆ f64        ┆ cat        │
    #   # ╞═════╪════════════╪════════════╡
    #   # │ -2  ┆ -1.0       ┆ (-inf, -1] │
    #   # │ -1  ┆ -1.0       ┆ (-inf, -1] │
    #   # │ 0   ┆ 1.0        ┆ (-1, 1]    │
    #   # │ 1   ┆ 1.0        ┆ (-1, 1]    │
    #   # │ 2   ┆ inf        ┆ (1, inf]   │
    #   # └─────┴────────────┴────────────┘
    def qcut(quantiles, labels: nil, left_closed: false, allow_duplicates: false, include_breaks: false)
      if quantiles.is_a?(Integer)
        rbexpr = _rbexpr.qcut_uniform(
          quantiles, labels, left_closed, allow_duplicates, include_breaks
        )
      else
        rbexpr = _rbexpr.qcut(
          quantiles, labels, left_closed, allow_duplicates, include_breaks
        )
      end

      _from_rbexpr(rbexpr)
    end

    # Get the lengths of runs of identical values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(Polars::Series.new("s", [1, 1, 2, 1, nil, 1, 3, 3]))
    #   df.select(Polars.col("s").rle).unnest("s")
    #   # =>
    #   # shape: (6, 2)
    #   # ┌─────┬───────┐
    #   # │ len ┆ value │
    #   # │ --- ┆ ---   │
    #   # │ u32 ┆ i64   │
    #   # ╞═════╪═══════╡
    #   # │ 2   ┆ 1     │
    #   # │ 1   ┆ 2     │
    #   # │ 1   ┆ 1     │
    #   # │ 1   ┆ null  │
    #   # │ 1   ┆ 1     │
    #   # │ 2   ┆ 3     │
    #   # └─────┴───────┘
    def rle
      _from_rbexpr(_rbexpr.rle)
    end

    # Map values to run IDs.
    #
    # Similar to RLE, but it maps each value to an ID corresponding to the run into
    # which it falls. This is especially useful when you want to define groups by
    # runs of identical values rather than the values themselves.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 1, 1, 1], "b" => ["x", "x", nil, "y", "y"]})
    #   df.with_columns([Polars.col("a").rle_id.alias("a_r"), Polars.struct(["a", "b"]).rle_id.alias("ab_r")])
    #   # =>
    #   # shape: (5, 4)
    #   # ┌─────┬──────┬─────┬──────┐
    #   # │ a   ┆ b    ┆ a_r ┆ ab_r │
    #   # │ --- ┆ ---  ┆ --- ┆ ---  │
    #   # │ i64 ┆ str  ┆ u32 ┆ u32  │
    #   # ╞═════╪══════╪═════╪══════╡
    #   # │ 1   ┆ x    ┆ 0   ┆ 0    │
    #   # │ 2   ┆ x    ┆ 1   ┆ 1    │
    #   # │ 1   ┆ null ┆ 2   ┆ 2    │
    #   # │ 1   ┆ y    ┆ 2   ┆ 3    │
    #   # │ 1   ┆ y    ┆ 2   ┆ 3    │
    #   # └─────┴──────┴─────┴──────┘
    def rle_id
      _from_rbexpr(_rbexpr.rle_id)
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
    #     df.group_by("group_col").agg(
    #       [
    #         Polars.col("b").filter(Polars.col("b") < 2).sum.alias("lt"),
    #         Polars.col("b").filter(Polars.col("b") >= 2).sum.alias("gte")
    #       ]
    #     )
    #   ).sort("group_col")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────────┬─────┬─────┐
    #   # │ group_col ┆ lt  ┆ gte │
    #   # │ ---       ┆ --- ┆ --- │
    #   # │ str       ┆ i64 ┆ i64 │
    #   # ╞═══════════╪═════╪═════╡
    #   # │ g1        ┆ 1   ┆ 2   │
    #   # │ g2        ┆ 0   ┆ 3   │
    #   # └───────────┴─────┴─────┘
    def filter(predicate)
      _from_rbexpr(_rbexpr.filter(predicate._rbexpr))
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
    #     df.group_by("group_col").agg(
    #       [
    #         Polars.col("b").where(Polars.col("b") < 2).sum.alias("lt"),
    #         Polars.col("b").where(Polars.col("b") >= 2).sum.alias("gte")
    #       ]
    #     )
    #   ).sort("group_col")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌───────────┬─────┬─────┐
    #   # │ group_col ┆ lt  ┆ gte │
    #   # │ ---       ┆ --- ┆ --- │
    #   # │ str       ┆ i64 ┆ i64 │
    #   # ╞═══════════╪═════╪═════╡
    #   # │ g1        ┆ 1   ┆ 2   │
    #   # │ g2        ┆ 0   ┆ 3   │
    #   # └───────────┴─────┴─────┘
    def where(predicate)
      filter(predicate)
    end

    # Apply a custom Ruby function to a Series or sequence of Series.
    #
    # The output of this custom function must be a Series.
    # If you want to apply a custom function elementwise over single values, see
    # {#apply}. A use case for `map` is when you want to transform an
    # expression with a third-party library.
    #
    # Read more in [the book](https://pola-rs.github.io/polars-book/user-guide/dsl/custom_functions.html).
    #
    # @param return_dtype [Symbol]
    #   Dtype of the output Series.
    # @param agg_list [Boolean]
    #   Aggregate list.
    # @param is_elementwise [Boolean]
    #   If set to true this can run in the streaming engine, but may yield
    #   incorrect results in group-by. Ensure you know what you are doing!
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "sine" => [0.0, 1.0, 0.0, -1.0],
    #       "cosine" => [1.0, 0.0, -1.0, 0.0]
    #     }
    #   )
    #   df.select(Polars.all.map { |x| x.to_numpy.argmax })
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────┬────────┐
    #   # │ sine ┆ cosine │
    #   # │ ---  ┆ ---    │
    #   # │ i64  ┆ i64    │
    #   # ╞══════╪════════╡
    #   # │ 1    ┆ 0      │
    #   # └──────┴────────┘
    # def map_batches(return_dtype: nil, agg_list: false, is_elementwise: false, returns_scalar: false, &f)
    #   if !return_dtype.nil?
    #     return_dtype = Utils.rb_type_to_dtype(return_dtype)
    #   end
    #   _from_rbexpr(
    #     _rbexpr.map_batches(
    #       # TODO _map_batches_wrapper
    #       f,
    #       return_dtype,
    #       agg_list,
    #       is_elementwise,
    #       returns_scalar
    #     )
    #   )
    # end
    # alias_method :map, :map_batches

    # Apply a custom/user-defined function (UDF) in a GroupBy or Projection context.
    #
    # Depending on the context it has the following behavior:
    #
    # * Selection
    #     Expects `f` to be of type Callable[[Any], Any].
    #     Applies a Ruby function over each individual value in the column.
    # * GroupBy
    #     Expects `f` to be of type Callable[[Series], Series].
    #     Applies a Ruby function over each group.
    #
    # Implementing logic using a Ruby function is almost always _significantly_
    # slower and more memory intensive than implementing the same logic using
    # the native expression API because:
    #
    # - The native expression engine runs in Rust; UDFs run in Ruby.
    # - Use of Ruby UDFs forces the DataFrame to be materialized in memory.
    # - Polars-native expressions can be parallelised (UDFs cannot).
    # - Polars-native expressions can be logically optimised (UDFs cannot).
    #
    # Wherever possible you should strongly prefer the native expression API
    # to achieve the best performance.
    #
    # @param return_dtype [Symbol]
    #   Dtype of the output Series.
    #   If not set, polars will assume that
    #   the dtype remains unchanged.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 1],
    #       "b" => ["a", "b", "c", "c"]
    #     }
    #   )
    #
    # @example In a selection context, the function is applied by row.
    #   df.with_column(
    #     Polars.col("a").map_elements { |x| x * 2 }.alias("a_times_2")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬───────────┐
    #   # │ a   ┆ b   ┆ a_times_2 │
    #   # │ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ str ┆ i64       │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ a   ┆ 2         │
    #   # │ 2   ┆ b   ┆ 4         │
    #   # │ 3   ┆ c   ┆ 6         │
    #   # │ 1   ┆ c   ┆ 2         │
    #   # └─────┴─────┴───────────┘
    #
    # @example In a GroupBy context the function is applied by group:
    #   df.lazy
    #     .group_by("b", maintain_order: true)
    #     .agg(
    #       [
    #         Polars.col("a").map_elements { |x| x.sum }
    #       ]
    #     )
    #     .collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ b   ┆ a   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 1   │
    #   # │ b   ┆ 2   │
    #   # │ c   ┆ 4   │
    #   # └─────┴─────┘
    # def map_elements(
    #   return_dtype: nil,
    #   skip_nulls: true,
    #   pass_name: false,
    #   strategy: "thread_local",
    #   &f
    # )
    #   if pass_name
    #     raise Todo
    #   else
    #     wrap_f = lambda do |x|
    #       x.map_elements(return_dtype: return_dtype, skip_nulls: skip_nulls, &f)
    #     end
    #   end
    #   map_batches(agg_list: true, return_dtype: return_dtype, &wrap_f)
    # end
    # alias_method :apply, :map_elements

    # Explode a list or utf8 Series. This means that every item is expanded to a new
    # row.
    #
    # Alias for {#explode}.
    #
    # @return [Expr]
    #
    # @example
    #  df = Polars::DataFrame.new(
    #    {
    #      "group" => ["a", "b", "b"],
    #      "values" => [[1, 2], [2, 3], [4]]
    #    }
    #  )
    #  df.group_by("group").agg(Polars.col("values").flatten)
    #  # =>
    #  # shape: (2, 2)
    #  # ┌───────┬───────────┐
    #  # │ group ┆ values    │
    #  # │ ---   ┆ ---       │
    #  # │ str   ┆ list[i64] │
    #  # ╞═══════╪═══════════╡
    #  # │ a     ┆ [1, 2]    │
    #  # │ b     ┆ [2, 3, 4] │
    #  # └───────┴───────────┘
    def flatten
      _from_rbexpr(_rbexpr.explode)
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
    #   # │ 2   │
    #   # │ 3   │
    #   # │ 4   │
    #   # │ 5   │
    #   # │ 6   │
    #   # └─────┘
    def explode
      _from_rbexpr(_rbexpr.explode)
    end

    # Take every nth value in the Series and return as a new Series.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5, 6, 7, 8, 9]})
    #   df.select(Polars.col("foo").gather_every(3))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 4   │
    #   # │ 7   │
    #   # └─────┘
    def gather_every(n, offset = 0)
      _from_rbexpr(_rbexpr.gather_every(n, offset))
    end
    alias_method :take_every, :gather_every

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
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    def head(n = 10)
      _from_rbexpr(_rbexpr.head(n))
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
    #   # │ 6   │
    #   # │ 7   │
    #   # └─────┘
    def tail(n = 10)
      _from_rbexpr(_rbexpr.tail(n))
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5, 6, 7]})
    #   df.select(Polars.col("foo").limit(3))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    def limit(n = 10)
      head(n)
    end

    # Method equivalent of equality operator `expr == other`.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [1.0, 2.0, Float::NAN, 4.0],
    #       "y" => [2.0, 2.0, Float::NAN, 4.0]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").eq(Polars.col("y")).alias("x == y")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬────────┐
    #   # │ x   ┆ y   ┆ x == y │
    #   # │ --- ┆ --- ┆ ---    │
    #   # │ f64 ┆ f64 ┆ bool   │
    #   # ╞═════╪═════╪════════╡
    #   # │ 1.0 ┆ 2.0 ┆ false  │
    #   # │ 2.0 ┆ 2.0 ┆ true   │
    #   # │ NaN ┆ NaN ┆ true   │
    #   # │ 4.0 ┆ 4.0 ┆ true   │
    #   # └─────┴─────┴────────┘
    def eq(other)
      self == other
    end

    # Method equivalent of equality operator `expr == other` where `None == None`.
    #
    # This differs from default `eq` where null values are propagated.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     data={
    #       "x" => [1.0, 2.0, Float::NAN, 4.0, nil, nil],
    #       "y" => [2.0, 2.0, Float::NAN, 4.0, 5.0, nil]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").eq(Polars.col("y")).alias("x eq y"),
    #     Polars.col("x").eq_missing(Polars.col("y")).alias("x eq_missing y")
    #   )
    #   # =>
    #   # shape: (6, 4)
    #   # ┌──────┬──────┬────────┬────────────────┐
    #   # │ x    ┆ y    ┆ x eq y ┆ x eq_missing y │
    #   # │ ---  ┆ ---  ┆ ---    ┆ ---            │
    #   # │ f64  ┆ f64  ┆ bool   ┆ bool           │
    #   # ╞══════╪══════╪════════╪════════════════╡
    #   # │ 1.0  ┆ 2.0  ┆ false  ┆ false          │
    #   # │ 2.0  ┆ 2.0  ┆ true   ┆ true           │
    #   # │ NaN  ┆ NaN  ┆ true   ┆ true           │
    #   # │ 4.0  ┆ 4.0  ┆ true   ┆ true           │
    #   # │ null ┆ 5.0  ┆ null   ┆ false          │
    #   # │ null ┆ null ┆ null   ┆ true           │
    #   # └──────┴──────┴────────┴────────────────┘
    def eq_missing(other)
      other = Utils.parse_into_expression(other, str_as_lit: true)
      _from_rbexpr(_rbexpr.eq_missing(other))
    end

    # Method equivalent of "greater than or equal" operator `expr >= other`.
    #
    # @param other [Object]
    #     A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [5.0, 4.0, Float::NAN, 2.0],
    #       "y" => [5.0, 3.0, Float::NAN, 1.0]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").ge(Polars.col("y")).alias("x >= y")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬────────┐
    #   # │ x   ┆ y   ┆ x >= y │
    #   # │ --- ┆ --- ┆ ---    │
    #   # │ f64 ┆ f64 ┆ bool   │
    #   # ╞═════╪═════╪════════╡
    #   # │ 5.0 ┆ 5.0 ┆ true   │
    #   # │ 4.0 ┆ 3.0 ┆ true   │
    #   # │ NaN ┆ NaN ┆ true   │
    #   # │ 2.0 ┆ 1.0 ┆ true   │
    #   # └─────┴─────┴────────┘
    def ge(other)
      self >= other
    end

    # Method equivalent of "greater than" operator `expr > other`.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [5.0, 4.0, Float::NAN, 2.0],
    #       "y" => [5.0, 3.0, Float::NAN, 1.0]
    #     }
    #   )
    #   df.with_columns(
    #       Polars.col("x").gt(Polars.col("y")).alias("x > y")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ x   ┆ y   ┆ x > y │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ f64 ┆ f64 ┆ bool  │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 5.0 ┆ 5.0 ┆ false │
    #   # │ 4.0 ┆ 3.0 ┆ true  │
    #   # │ NaN ┆ NaN ┆ false │
    #   # │ 2.0 ┆ 1.0 ┆ true  │
    #   # └─────┴─────┴───────┘
    def gt(other)
      self > other
    end

    # Method equivalent of "less than or equal" operator `expr <= other`.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [5.0, 4.0, Float::NAN, 0.5],
    #       "y" => [5.0, 3.5, Float::NAN, 2.0]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").le(Polars.col("y")).alias("x <= y")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬────────┐
    #   # │ x   ┆ y   ┆ x <= y │
    #   # │ --- ┆ --- ┆ ---    │
    #   # │ f64 ┆ f64 ┆ bool   │
    #   # ╞═════╪═════╪════════╡
    #   # │ 5.0 ┆ 5.0 ┆ true   │
    #   # │ 4.0 ┆ 3.5 ┆ false  │
    #   # │ NaN ┆ NaN ┆ true   │
    #   # │ 0.5 ┆ 2.0 ┆ true   │
    #   # └─────┴─────┴────────┘
    def le(other)
      self <= other
    end

    # Method equivalent of "less than" operator `expr < other`.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [1.0, 2.0, Float::NAN, 3.0],
    #       "y" => [2.0, 2.0, Float::NAN, 4.0]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").lt(Polars.col("y")).alias("x < y"),
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ x   ┆ y   ┆ x < y │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ f64 ┆ f64 ┆ bool  │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 1.0 ┆ 2.0 ┆ true  │
    #   # │ 2.0 ┆ 2.0 ┆ false │
    #   # │ NaN ┆ NaN ┆ false │
    #   # │ 3.0 ┆ 4.0 ┆ true  │
    #   # └─────┴─────┴───────┘
    def lt(other)
      self < other
    end

    # Method equivalent of inequality operator `expr != other`.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [1.0, 2.0, Float::NAN, 4.0],
    #       "y" => [2.0, 2.0, Float::NAN, 4.0]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").ne(Polars.col("y")).alias("x != y"),
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬────────┐
    #   # │ x   ┆ y   ┆ x != y │
    #   # │ --- ┆ --- ┆ ---    │
    #   # │ f64 ┆ f64 ┆ bool   │
    #   # ╞═════╪═════╪════════╡
    #   # │ 1.0 ┆ 2.0 ┆ true   │
    #   # │ 2.0 ┆ 2.0 ┆ false  │
    #   # │ NaN ┆ NaN ┆ false  │
    #   # │ 4.0 ┆ 4.0 ┆ false  │
    #   # └─────┴─────┴────────┘
    def ne(other)
      self != other
    end

    # Method equivalent of equality operator `expr != other` where `None == None`.
    #
    # This differs from default `ne` where null values are propagated.
    #
    # @param other [Object]
    #   A literal or expression value to compare with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => [1.0, 2.0, Float::NAN, 4.0, nil, nil],
    #       "y" => [2.0, 2.0, Float::NAN, 4.0, 5.0, nil]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("x").ne(Polars.col("y")).alias("x ne y"),
    #     Polars.col("x").ne_missing(Polars.col("y")).alias("x ne_missing y")
    #   )
    #   # =>
    #   # shape: (6, 4)
    #   # ┌──────┬──────┬────────┬────────────────┐
    #   # │ x    ┆ y    ┆ x ne y ┆ x ne_missing y │
    #   # │ ---  ┆ ---  ┆ ---    ┆ ---            │
    #   # │ f64  ┆ f64  ┆ bool   ┆ bool           │
    #   # ╞══════╪══════╪════════╪════════════════╡
    #   # │ 1.0  ┆ 2.0  ┆ true   ┆ true           │
    #   # │ 2.0  ┆ 2.0  ┆ false  ┆ false          │
    #   # │ NaN  ┆ NaN  ┆ false  ┆ false          │
    #   # │ 4.0  ┆ 4.0  ┆ false  ┆ false          │
    #   # │ null ┆ 5.0  ┆ null   ┆ true           │
    #   # │ null ┆ null ┆ null   ┆ false          │
    #   # └──────┴──────┴────────┴────────────────┘
    def ne_missing(other)
      other = Utils.parse_into_expression(other, str_as_lit: true)
      _from_rbexpr(_rbexpr.neq_missing(other))
    end

    # Method equivalent of addition operator `expr + other`.
    #
    # @param other [Object]
    #   numeric or string value; accepts expression input.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => [1, 2, 3, 4, 5]})
    #   df.with_columns(
    #     Polars.col("x").add(2).alias("x+int"),
    #     Polars.col("x").add(Polars.col("x").cum_prod).alias("x+expr")
    #   )
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬───────┬────────┐
    #   # │ x   ┆ x+int ┆ x+expr │
    #   # │ --- ┆ ---   ┆ ---    │
    #   # │ i64 ┆ i64   ┆ i64    │
    #   # ╞═════╪═══════╪════════╡
    #   # │ 1   ┆ 3     ┆ 2      │
    #   # │ 2   ┆ 4     ┆ 4      │
    #   # │ 3   ┆ 5     ┆ 9      │
    #   # │ 4   ┆ 6     ┆ 28     │
    #   # │ 5   ┆ 7     ┆ 125    │
    #   # └─────┴───────┴────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"x" => ["a", "d", "g"], "y": ["b", "e", "h"], "z": ["c", "f", "i"]}
    #   )
    #   df.with_columns(Polars.col("x").add(Polars.col("y")).add(Polars.col("z")).alias("xyz"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬─────┐
    #   # │ x   ┆ y   ┆ z   ┆ xyz │
    #   # │ --- ┆ --- ┆ --- ┆ --- │
    #   # │ str ┆ str ┆ str ┆ str │
    #   # ╞═════╪═════╪═════╪═════╡
    #   # │ a   ┆ b   ┆ c   ┆ abc │
    #   # │ d   ┆ e   ┆ f   ┆ def │
    #   # │ g   ┆ h   ┆ i   ┆ ghi │
    #   # └─────┴─────┴─────┴─────┘
    def add(other)
      self + other
    end

    # Method equivalent of integer division operator `expr // other`.
    #
    # @param other [Object]
    #   Numeric literal or expression value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => [1, 2, 3, 4, 5]})
    #   df.with_columns(
    #     Polars.col("x").truediv(2).alias("x/2"),
    #     Polars.col("x").floordiv(2).alias("x//2")
    #   )
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ x   ┆ x/2 ┆ x//2 │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ i64  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1   ┆ 0.5 ┆ 0    │
    #   # │ 2   ┆ 1.0 ┆ 1    │
    #   # │ 3   ┆ 1.5 ┆ 1    │
    #   # │ 4   ┆ 2.0 ┆ 2    │
    #   # │ 5   ┆ 2.5 ┆ 2    │
    #   # └─────┴─────┴──────┘
    def floordiv(other)
      _from_rbexpr(_rbexpr.floordiv(_to_rbexpr(other)))
    end

    # Method equivalent of modulus operator `expr % other`.
    #
    # @param other [Object]
    #   Numeric literal or expression value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => [0, 1, 2, 3, 4]})
    #   df.with_columns(Polars.col("x").mod(2).alias("x%2"))
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ x   ┆ x%2 │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 0   ┆ 0   │
    #   # │ 1   ┆ 1   │
    #   # │ 2   ┆ 0   │
    #   # │ 3   ┆ 1   │
    #   # │ 4   ┆ 0   │
    #   # └─────┴─────┘
    def mod(other)
      self % other
    end

    # Method equivalent of multiplication operator `expr * other`.
    #
    # @param other [Object]
    #   Numeric literal or expression value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => [1, 2, 4, 8, 16]})
    #   df.with_columns(
    #     Polars.col("x").mul(2).alias("x*2"),
    #     Polars.col("x").mul(Polars.col("x").log(2)).alias("x * xlog2"),
    #   )
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬─────┬───────────┐
    #   # │ x   ┆ x*2 ┆ x * xlog2 │
    #   # │ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64 ┆ f64       │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ 2   ┆ 0.0       │
    #   # │ 2   ┆ 4   ┆ 2.0       │
    #   # │ 4   ┆ 8   ┆ 8.0       │
    #   # │ 8   ┆ 16  ┆ 24.0      │
    #   # │ 16  ┆ 32  ┆ 64.0      │
    #   # └─────┴─────┴───────────┘
    def mul(other)
      self * other
    end

    # Method equivalent of subtraction operator `expr - other`.
    #
    # @param other [Object]
    #   Numeric literal or expression value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => [0, 1, 2, 3, 4]})
    #   df.with_columns(
    #     Polars.col("x").sub(2).alias("x-2"),
    #     Polars.col("x").sub(Polars.col("x").cum_sum).alias("x-expr"),
    #   )
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬─────┬────────┐
    #   # │ x   ┆ x-2 ┆ x-expr │
    #   # │ --- ┆ --- ┆ ---    │
    #   # │ i64 ┆ i64 ┆ i64    │
    #   # ╞═════╪═════╪════════╡
    #   # │ 0   ┆ -2  ┆ 0      │
    #   # │ 1   ┆ -1  ┆ 0      │
    #   # │ 2   ┆ 0   ┆ -1     │
    #   # │ 3   ┆ 1   ┆ -3     │
    #   # │ 4   ┆ 2   ┆ -6     │
    #   # └─────┴─────┴────────┘
    def sub(other)
      self - other
    end

    # Method equivalent of unary minus operator `-expr`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 0, 2, nil]})
    #   df.with_columns(Polars.col("a").neg)
    #   # =>
    #   # shape: (4, 1)
    #   # ┌──────┐
    #   # │ a    │
    #   # │ ---  │
    #   # │ i64  │
    #   # ╞══════╡
    #   # │ 1    │
    #   # │ 0    │
    #   # │ -2   │
    #   # │ null │
    #   # └──────┘
    def neg
      -self
    end

    # Method equivalent of float division operator `expr / other`.
    #
    # @param other [Object]
    #   Numeric literal or expression value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"x" => [-2, -1, 0, 1, 2], "y" => [0.5, 0.0, 0.0, -4.0, -0.5]}
    #   )
    #   df.with_columns(
    #     Polars.col("x").truediv(2).alias("x/2"),
    #     Polars.col("x").truediv(Polars.col("y")).alias("x/y")
    #   )
    #   # =>
    #   # shape: (5, 4)
    #   # ┌─────┬──────┬──────┬───────┐
    #   # │ x   ┆ y    ┆ x/2  ┆ x/y   │
    #   # │ --- ┆ ---  ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ f64  ┆ f64   │
    #   # ╞═════╪══════╪══════╪═══════╡
    #   # │ -2  ┆ 0.5  ┆ -1.0 ┆ -4.0  │
    #   # │ -1  ┆ 0.0  ┆ -0.5 ┆ -inf  │
    #   # │ 0   ┆ 0.0  ┆ 0.0  ┆ NaN   │
    #   # │ 1   ┆ -4.0 ┆ 0.5  ┆ -0.25 │
    #   # │ 2   ┆ -0.5 ┆ 1.0  ┆ -4.0  │
    #   # └─────┴──────┴──────┴───────┘
    def truediv(other)
      self / other
    end

    # Raise expression to the power of exponent.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => [1, 2, 4, 8]})
    #   df.with_columns(
    #     Polars.col("x").pow(3).alias("cube"),
    #     Polars.col("x").pow(Polars.col("x").log(2)).alias("x ** xlog2")
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬──────┬────────────┐
    #   # │ x   ┆ cube ┆ x ** xlog2 │
    #   # │ --- ┆ ---  ┆ ---        │
    #   # │ i64 ┆ i64  ┆ f64        │
    #   # ╞═════╪══════╪════════════╡
    #   # │ 1   ┆ 1    ┆ 1.0        │
    #   # │ 2   ┆ 8    ┆ 2.0        │
    #   # │ 4   ┆ 64   ┆ 16.0       │
    #   # │ 8   ┆ 512  ┆ 512.0      │
    #   # └─────┴──────┴────────────┘
    def pow(exponent)
      self**exponent
    end

    # Method equivalent of bitwise exclusive-or operator `expr ^ other`.
    #
    # @param other [Object]
    #   Integer or boolean value; accepts expression input.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"x" => [true, false, true, false], "y" => [true, true, false, false]}
    #   )
    #   df.with_columns(Polars.col("x").xor(Polars.col("y")).alias("x ^ y"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌───────┬───────┬───────┐
    #   # │ x     ┆ y     ┆ x ^ y │
    #   # │ ---   ┆ ---   ┆ ---   │
    #   # │ bool  ┆ bool  ┆ bool  │
    #   # ╞═══════╪═══════╪═══════╡
    #   # │ true  ┆ true  ┆ false │
    #   # │ false ┆ true  ┆ true  │
    #   # │ true  ┆ false ┆ true  │
    #   # │ false ┆ false ┆ false │
    #   # └───────┴───────┴───────┘
    def xor(other)
      self ^ other
    end

    # Check if elements of this expression are present in the other Series.
    #
    # @param other [Object]
    #   Series or sequence of primitive type.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"sets" => [[1, 2, 3], [1, 2], [9, 10]], "optional_members" => [1, 2, 3]}
    #   )
    #   df.select([Polars.col("optional_members").is_in("sets").alias("contains")])
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ contains │
    #   # │ ---      │
    #   # │ bool     │
    #   # ╞══════════╡
    #   # │ true     │
    #   # │ true     │
    #   # │ false    │
    #   # └──────────┘
    def is_in(other)
      if other.is_a?(::Array)
        if other.length == 0
          other = Polars.lit(nil)._rbexpr
        else
          other = Polars.lit(Series.new(other))._rbexpr
        end
      else
        other = Utils.parse_into_expression(other, str_as_lit: false)
      end
      _from_rbexpr(_rbexpr.is_in(other))
    end
    alias_method :in?, :is_in

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
    #   # │ ["y", "y"]      │
    #   # │ ["z", "z", "z"] │
    #   # └─────────────────┘
    def repeat_by(by)
      by = Utils.parse_into_expression(by, str_as_lit: false)
      _from_rbexpr(_rbexpr.repeat_by(by))
    end

    # Check if this expression is between start and end.
    #
    # @param lower_bound [Object]
    #   Lower bound as primitive type or datetime.
    # @param upper_bound [Object]
    #   Upper bound as primitive type or datetime.
    # @param closed ["both", "left", "right", "none"]
    #   Define which sides of the interval are closed (inclusive).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"num" => [1, 2, 3, 4, 5]})
    #   df.with_columns(Polars.col("num").is_between(2, 4).alias("is_between"))
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬────────────┐
    #   # │ num ┆ is_between │
    #   # │ --- ┆ ---        │
    #   # │ i64 ┆ bool       │
    #   # ╞═════╪════════════╡
    #   # │ 1   ┆ false      │
    #   # │ 2   ┆ true       │
    #   # │ 3   ┆ true       │
    #   # │ 4   ┆ true       │
    #   # │ 5   ┆ false      │
    #   # └─────┴────────────┘
    #
    # @example Use the `closed` argument to include or exclude the values at the bounds:
    #   df.with_columns(
    #     Polars.col("num").is_between(2, 4, closed: "left").alias("is_between")
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬────────────┐
    #   # │ num ┆ is_between │
    #   # │ --- ┆ ---        │
    #   # │ i64 ┆ bool       │
    #   # ╞═════╪════════════╡
    #   # │ 1   ┆ false      │
    #   # │ 2   ┆ true       │
    #   # │ 3   ┆ true       │
    #   # │ 4   ┆ false      │
    #   # │ 5   ┆ false      │
    #   # └─────┴────────────┘
    #
    # @example You can also use strings as well as numeric/temporal values:
    #   df = Polars::DataFrame.new({"a" => ["a", "b", "c", "d", "e"]})
    #   df.with_columns(
    #     Polars.col("a")
    #       .is_between(Polars.lit("a"), Polars.lit("c"), closed: "both")
    #       .alias("is_between")
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬────────────┐
    #   # │ a   ┆ is_between │
    #   # │ --- ┆ ---        │
    #   # │ str ┆ bool       │
    #   # ╞═════╪════════════╡
    #   # │ a   ┆ true       │
    #   # │ b   ┆ true       │
    #   # │ c   ┆ true       │
    #   # │ d   ┆ false      │
    #   # │ e   ┆ false      │
    #   # └─────┴────────────┘
    def is_between(lower_bound, upper_bound, closed: "both")
      lower_bound = Utils.parse_into_expression(lower_bound)
      upper_bound = Utils.parse_into_expression(upper_bound)

      _from_rbexpr(
        _rbexpr.is_between(lower_bound, upper_bound, closed)
      )
    end

    # Hash the elements in the selection.
    #
    # The hash value is of type `:u64`.
    #
    # @param seed [Integer]
    #   Random seed parameter. Defaults to 0.
    # @param seed_1 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    # @param seed_2 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    # @param seed_3 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil],
    #       "b" => ["x", nil, "z"]
    #     }
    #   )
    #   df.with_column(Polars.all._hash(10, 20, 30, 40))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────────────────────┬──────────────────────┐
    #   # │ a                    ┆ b                    │
    #   # │ ---                  ┆ ---                  │
    #   # │ u64                  ┆ u64                  │
    #   # ╞══════════════════════╪══════════════════════╡
    #   # │ 4629889412789719550  ┆ 6959506404929392568  │
    #   # │ 16386608652769605760 ┆ 11638928888656214026 │
    #   # │ 11638928888656214026 ┆ 11040941213715918520 │
    #   # └──────────────────────┴──────────────────────┘
    def _hash(seed = 0, seed_1 = nil, seed_2 = nil, seed_3 = nil)
      k0 = seed
      k1 = seed_1.nil? ? seed : seed_1
      k2 = seed_2.nil? ? seed : seed_2
      k3 = seed_3.nil? ? seed : seed_3
      _from_rbexpr(_rbexpr._hash(k0, k1, k2, k3))
    end

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
    #   # │ 1             ┆ 1        │
    #   # │ 2             ┆ 2        │
    #   # └───────────────┴──────────┘
    def reinterpret(signed: false)
      _from_rbexpr(_rbexpr.reinterpret(signed))
    end

    # Print the value that this expression evaluates to and pass on the value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 1, 2]})
    #   df.select(Polars.col("foo").cumsum._inspect("value is: %s").alias("bar"))
    #   # =>
    #   # value is: shape: (3,)
    #   # Series: 'foo' [i64]
    #   # [
    #   #     1
    #   #     2
    #   #     4
    #   # ]
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ bar │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 4   │
    #   # └─────┘
    # def _inspect(fmt = "%s")
    #   inspect = lambda do |s|
    #     puts(fmt % [s])
    #     s
    #   end

    #   map(return_dtype: nil, agg_list: true, &inspect)
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
    #   # │ f64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 1.0 ┆ 1.0 │
    #   # │ 2.0 ┆ NaN │
    #   # │ 3.0 ┆ 3.0 │
    #   # └─────┴─────┘
    def interpolate(method: "linear")
      _from_rbexpr(_rbexpr.interpolate(method))
    end

    # Fill null values using interpolation based on another column.
    #
    # @return [Expr]
    #
    # @param by [Expr] Column to interpolate values based on.
    #
    # @example Fill null values using linear interpolation.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, nil, nil, 3],
    #       "b" => [1, 2, 7, 8]
    #     }
    #   )
    #   df.with_columns(a_interpolated: Polars.col("a").interpolate_by("b"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬─────┬────────────────┐
    #   # │ a    ┆ b   ┆ a_interpolated │
    #   # │ ---  ┆ --- ┆ ---            │
    #   # │ i64  ┆ i64 ┆ f64            │
    #   # ╞══════╪═════╪════════════════╡
    #   # │ 1    ┆ 1   ┆ 1.0            │
    #   # │ null ┆ 2   ┆ 1.285714       │
    #   # │ null ┆ 7   ┆ 2.714286       │
    #   # │ 3    ┆ 8   ┆ 3.0            │
    #   # └──────┴─────┴────────────────┘
    def interpolate_by(by)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(_rbexpr.interpolate_by(by))
    end

    # Apply a rolling min based on another column.
    #
    # @param by [String]
    #   This column must be of dtype Datetime or Date.
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #     {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling min with the temporal windows closed on the right (default)
    #   df_temporal.with_columns(
    #     rolling_row_min: Polars.col("index").rolling_min_by("date", "2h")
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_min │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ u32             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0               │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0               │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1               │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 2               │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 3               │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 19              │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 20              │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 21              │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 22              │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 23              │
    #   # └───────┴─────────────────────┴─────────────────┘
    def rolling_min_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_min_by(by, window_size, min_periods, closed)
      )
    end

    # Apply a rolling max based on another column.
    #
    # @param by [String]
    #   This column must be of dtype Datetime or Date.
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #       {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling max with the temporal windows closed on the right (default)
    #   df_temporal.with_columns(
    #     rolling_row_max: Polars.col("index").rolling_max_by("date", "2h")
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_max │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ u32             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0               │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 1               │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 2               │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 3               │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 4               │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 20              │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 21              │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 22              │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 23              │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 24              │
    #   # └───────┴─────────────────────┴─────────────────┘
    #
    # @example Compute the rolling max with the closure of windows on both sides
    #   df_temporal.with_columns(
    #     rolling_row_max: Polars.col("index").rolling_max_by(
    #       "date", "2h", closed: "both"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_max │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ u32             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0               │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 1               │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 2               │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 3               │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 4               │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 20              │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 21              │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 22              │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 23              │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 24              │
    #   # └───────┴─────────────────────┴─────────────────┘
    def rolling_max_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_max_by(by, window_size, min_periods, closed)
      )
    end

    # Apply a rolling mean based on another column.
    #
    # @param by [String]
    #   This column must be of dtype Datetime or Date.
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #       {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling mean with the temporal windows closed on the right (default)
    #   df_temporal.with_columns(
    #     rolling_row_mean: Polars.col("index").rolling_mean_by(
    #       "date", "2h"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬──────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_mean │
    #   # │ ---   ┆ ---                 ┆ ---              │
    #   # │ u32   ┆ datetime[ns]        ┆ f64              │
    #   # ╞═══════╪═════════════════════╪══════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0.0              │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.5              │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1.5              │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 2.5              │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 3.5              │
    #   # │ …     ┆ …                   ┆ …                │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 19.5             │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 20.5             │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 21.5             │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 22.5             │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 23.5             │
    #   # └───────┴─────────────────────┴──────────────────┘
    #
    # @example Compute the rolling mean with the closure of windows on both sides
    #   df_temporal.with_columns(
    #     rolling_row_mean: Polars.col("index").rolling_mean_by(
    #       "date", "2h", closed: "both"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬──────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_mean │
    #   # │ ---   ┆ ---                 ┆ ---              │
    #   # │ u32   ┆ datetime[ns]        ┆ f64              │
    #   # ╞═══════╪═════════════════════╪══════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0.0              │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.5              │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1.0              │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 2.0              │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 3.0              │
    #   # │ …     ┆ …                   ┆ …                │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 19.0             │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 20.0             │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 21.0             │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 22.0             │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 23.0             │
    #   # └───────┴─────────────────────┴──────────────────┘
    def rolling_mean_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_mean_by(
          by,
          window_size,
          min_periods,
          closed
        )
      )
    end

    # Apply a rolling sum based on another column.
    #
    # @param by [String]
    #   This column must of dtype `{Date, Datetime}`
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #       {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling sum with the temporal windows closed on the right (default)
    #   df_temporal.with_columns(
    #     rolling_row_sum: Polars.col("index").rolling_sum_by("date", "2h")
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_sum │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ u32             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0               │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 1               │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 3               │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 5               │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 7               │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 39              │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 41              │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 43              │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 45              │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 47              │
    #   # └───────┴─────────────────────┴─────────────────┘
    #
    # @example Compute the rolling sum with the closure of windows on both sides
    #   df_temporal.with_columns(
    #     rolling_row_sum: Polars.col("index").rolling_sum_by(
    #       "date", "2h", closed: "both"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_sum │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ u32             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0               │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 1               │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 3               │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 6               │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 9               │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 57              │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 60              │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 63              │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 66              │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 69              │
    #   # └───────┴─────────────────────┴─────────────────┘
    def rolling_sum_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_sum_by(by, window_size, min_periods, closed)
      )
    end

    # Compute a rolling standard deviation based on another column.
    #
    # @param by [String]
    #   This column must be of dtype Datetime or Date.
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param ddof [Integer]
    #   "Delta Degrees of Freedom": The divisor for a length N window is N - ddof
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #       {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling std with the temporal windows closed on the right (default)
    #   df_temporal.with_columns(
    #     rolling_row_std: Polars.col("index").rolling_std_by("date", "2h")
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_std │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ f64             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ null            │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.707107        │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 0.707107        │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 0.707107        │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 0.707107        │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 0.707107        │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 0.707107        │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 0.707107        │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 0.707107        │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 0.707107        │
    #   # └───────┴─────────────────────┴─────────────────┘
    #
    # @example Compute the rolling std with the closure of windows on both sides
    #   df_temporal.with_columns(
    #     rolling_row_std: Polars.col("index").rolling_std_by(
    #       "date", "2h", closed: "both"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_std │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ f64             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ null            │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.707107        │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1.0             │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 1.0             │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 1.0             │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 1.0             │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 1.0             │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 1.0             │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 1.0             │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 1.0             │
    #   # └───────┴─────────────────────┴─────────────────┘
    def rolling_std_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      ddof: 1,
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_std_by(
          by,
          window_size,
          min_periods,
          closed,
          ddof
        )
      )
    end

    # Compute a rolling variance based on another column.
    #
    # @param by [String]
    #   This column must be of dtype Datetime or Date.
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param ddof [Integer]
    #   "Delta Degrees of Freedom": The divisor for a length N window is N - ddof
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #       {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling var with the temporal windows closed on the right (default)
    #   df_temporal.with_columns(
    #     rolling_row_var: Polars.col("index").rolling_var_by("date", "2h")
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_var │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ f64             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ null            │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.5             │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 0.5             │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 0.5             │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 0.5             │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 0.5             │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 0.5             │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 0.5             │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 0.5             │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 0.5             │
    #   # └───────┴─────────────────────┴─────────────────┘
    #
    # @example Compute the rolling var with the closure of windows on both sides
    #   df_temporal.with_columns(
    #     rolling_row_var: Polars.col("index").rolling_var_by(
    #       "date", "2h", closed: "both"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬─────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_var │
    #   # │ ---   ┆ ---                 ┆ ---             │
    #   # │ u32   ┆ datetime[ns]        ┆ f64             │
    #   # ╞═══════╪═════════════════════╪═════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ null            │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.5             │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1.0             │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 1.0             │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 1.0             │
    #   # │ …     ┆ …                   ┆ …               │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 1.0             │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 1.0             │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 1.0             │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 1.0             │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 1.0             │
    #   # └───────┴─────────────────────┴─────────────────┘
    def rolling_var_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      ddof: 1,
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_var_by(
          by,
          window_size,
          min_periods,
          closed,
          ddof
        )
      )
    end

    # Compute a rolling median based on another column.
    #
    # @param by [String]
    #     This column must be of dtype Datetime or Date.
    # @param window_size [String]
    #   The length of the window. Can be a dynamic temporal
    #   size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #     {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling median with the temporal windows closed on the right:
    #   df_temporal.with_columns(
    #     rolling_row_median: Polars.col("index").rolling_median_by(
    #       "date", "2h"
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬────────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_median │
    #   # │ ---   ┆ ---                 ┆ ---                │
    #   # │ u32   ┆ datetime[ns]        ┆ f64                │
    #   # ╞═══════╪═════════════════════╪════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0.0                │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.5                │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1.5                │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 2.5                │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 3.5                │
    #   # │ …     ┆ …                   ┆ …                  │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 19.5               │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 20.5               │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 21.5               │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 22.5               │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 23.5               │
    #   # └───────┴─────────────────────┴────────────────────┘
    def rolling_median_by(
      by,
      window_size,
      min_periods: 1,
      closed: "right",
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_median_by(by, window_size, min_periods, closed)
      )
    end

    # Compute a rolling quantile based on another column.
    #
    # @param by [String]
    #   This column must be of dtype Datetime or Date.
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ['nearest', 'higher', 'lower', 'midpoint', 'linear']
    #   Interpolation method.
    # @param window_size  [String]
    #   The length of the window. Can be a dynamic
    #   temporal size indicated by a timedelta or the following string language:
    #
    #   - 1ns   (1 nanosecond)
    #   - 1us   (1 microsecond)
    #   - 1ms   (1 millisecond)
    #   - 1s    (1 second)
    #   - 1m    (1 minute)
    #   - 1h    (1 hour)
    #   - 1d    (1 calendar day)
    #   - 1w    (1 calendar week)
    #   - 1mo   (1 calendar month)
    #   - 1q    (1 calendar quarter)
    #   - 1y    (1 calendar year)
    #
    #   By "calendar day", we mean the corresponding time on the next day
    #   (which may not be 24 hours, due to daylight savings). Similarly for
    #   "calendar week", "calendar month", "calendar quarter", and
    #   "calendar year".
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result.
    # @param closed ['left', 'right', 'both', 'none']
    #   Define which sides of the temporal interval are closed (inclusive),
    #   defaults to `'right'`.
    # @param warn_if_unsorted [Boolean]
    #   Warn if data is not known to be sorted by `by` column.
    #
    # @return [Expr]
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `rolling` - this method can cache the window size
    #   computation.
    #
    # @example Create a DataFrame with a datetime column and a row number column
    #   start = DateTime.new(2001, 1, 1)
    #   stop = DateTime.new(2001, 1, 2)
    #   df_temporal = Polars::DataFrame.new(
    #       {"date" => Polars.datetime_range(start, stop, "1h", eager: true)}
    #   ).with_row_index
    #   # =>
    #   # shape: (25, 2)
    #   # ┌───────┬─────────────────────┐
    #   # │ index ┆ date                │
    #   # │ ---   ┆ ---                 │
    #   # │ u32   ┆ datetime[ns]        │
    #   # ╞═══════╪═════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 │
    #   # │ 1     ┆ 2001-01-01 01:00:00 │
    #   # │ 2     ┆ 2001-01-01 02:00:00 │
    #   # │ 3     ┆ 2001-01-01 03:00:00 │
    #   # │ 4     ┆ 2001-01-01 04:00:00 │
    #   # │ …     ┆ …                   │
    #   # │ 20    ┆ 2001-01-01 20:00:00 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 │
    #   # └───────┴─────────────────────┘
    #
    # @example Compute the rolling quantile with the temporal windows closed on the right:
    #   df_temporal.with_columns(
    #     rolling_row_quantile: Polars.col("index").rolling_quantile_by(
    #       "date", "2h", quantile: 0.3
    #     )
    #   )
    #   # =>
    #   # shape: (25, 3)
    #   # ┌───────┬─────────────────────┬──────────────────────┐
    #   # │ index ┆ date                ┆ rolling_row_quantile │
    #   # │ ---   ┆ ---                 ┆ ---                  │
    #   # │ u32   ┆ datetime[ns]        ┆ f64                  │
    #   # ╞═══════╪═════════════════════╪══════════════════════╡
    #   # │ 0     ┆ 2001-01-01 00:00:00 ┆ 0.0                  │
    #   # │ 1     ┆ 2001-01-01 01:00:00 ┆ 0.0                  │
    #   # │ 2     ┆ 2001-01-01 02:00:00 ┆ 1.0                  │
    #   # │ 3     ┆ 2001-01-01 03:00:00 ┆ 2.0                  │
    #   # │ 4     ┆ 2001-01-01 04:00:00 ┆ 3.0                  │
    #   # │ …     ┆ …                   ┆ …                    │
    #   # │ 20    ┆ 2001-01-01 20:00:00 ┆ 19.0                 │
    #   # │ 21    ┆ 2001-01-01 21:00:00 ┆ 20.0                 │
    #   # │ 22    ┆ 2001-01-01 22:00:00 ┆ 21.0                 │
    #   # │ 23    ┆ 2001-01-01 23:00:00 ┆ 22.0                 │
    #   # │ 24    ┆ 2001-01-02 00:00:00 ┆ 23.0                 │
    #   # └───────┴─────────────────────┴──────────────────────┘
    def rolling_quantile_by(
      by,
      window_size,
      quantile:,
      interpolation: "nearest",
      min_periods: 1,
      closed: "right",
      warn_if_unsorted: nil
    )
      window_size = _prepare_rolling_by_window_args(window_size)
      by = Utils.parse_into_expression(by)
      _from_rbexpr(
        _rbexpr.rolling_quantile_by(
          by,
          quantile,
          interpolation,
          window_size,
          min_periods,
          closed,
        )
      )
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ 1.0  │
    #   # │ 2.0  │
    #   # │ 3.0  │
    #   # │ 4.0  │
    #   # │ 5.0  │
    #   # └──────┘
    def rolling_min(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      _from_rbexpr(
        _rbexpr.rolling_min(
          window_size, weights, min_periods, center
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ 2.0  │
    #   # │ 3.0  │
    #   # │ 4.0  │
    #   # │ 5.0  │
    #   # │ 6.0  │
    #   # └──────┘
    def rolling_max(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      _from_rbexpr(
        _rbexpr.rolling_max(
          window_size, weights, min_periods, center
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ 4.5  │
    #   # │ 7.0  │
    #   # │ 4.0  │
    #   # │ 9.0  │
    #   # │ 13.0 │
    #   # └──────┘
    def rolling_mean(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      _from_rbexpr(
        _rbexpr.rolling_mean(
          window_size, weights, min_periods, center
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ 3.0  │
    #   # │ 5.0  │
    #   # │ 7.0  │
    #   # │ 9.0  │
    #   # │ 11.0 │
    #   # └──────┘
    def rolling_sum(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      _from_rbexpr(
        _rbexpr.rolling_sum(
          window_size, weights, min_periods, center
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ null     │
    #   # │ 1.0      │
    #   # │ 1.0      │
    #   # │ 1.527525 │
    #   # │ 2.0      │
    #   # └──────────┘
    def rolling_std(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      ddof: 1
    )
      _from_rbexpr(
        _rbexpr.rolling_std(
          window_size, weights, min_periods, center, ddof
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ null     │
    #   # │ 1.0      │
    #   # │ 1.0      │
    #   # │ 2.333333 │
    #   # │ 4.0      │
    #   # └──────────┘
    def rolling_var(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false,
      ddof: 1
    )
      _from_rbexpr(
        _rbexpr.rolling_var(
          window_size, weights, min_periods, center, ddof
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ null │
    #   # │ 2.0  │
    #   # │ 3.0  │
    #   # │ 4.0  │
    #   # │ 6.0  │
    #   # └──────┘
    def rolling_median(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      _from_rbexpr(
        _rbexpr.rolling_median(
          window_size, weights, min_periods, center
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
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   If you want to compute multiple aggregation statistics over the same dynamic
    #   window, consider using `group_by_rolling` this method can cache the window size
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
    #   # │ null │
    #   # │ 1.0  │
    #   # │ 2.0  │
    #   # │ 3.0  │
    #   # │ 4.0  │
    #   # └──────┘
    def rolling_quantile(
      quantile,
      interpolation: "nearest",
      window_size: 2,
      weights: nil,
      min_periods: nil,
      center: false
    )
      _from_rbexpr(
        _rbexpr.rolling_quantile(
          quantile, interpolation, window_size, weights, min_periods, center
        )
      )
    end

    # Apply a custom rolling window function.
    #
    # Prefer the specific rolling window functions over this one, as they are faster.
    #
    # Prefer:
    # * rolling_min
    # * rolling_max
    # * rolling_mean
    # * rolling_sum
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Object]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If nil, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [1.0, 2.0, 9.0, 2.0, 13.0]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.col("A").rolling_apply(window_size: 3) { |s| s.std }
    #     ]
    #   )
    #   # =>
    #   # shape: (5, 1)
    #   # ┌──────────┐
    #   # │ A        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ null     │
    #   # │ null     │
    #   # │ 4.358899 │
    #   # │ 4.041452 │
    #   # │ 5.567764 │
    #   # └──────────┘
    # def rolling_apply(
    #   window_size:,
    #   weights: nil,
    #   min_periods: nil,
    #   center: false,
    #   &function
    # )
    #   if min_periods.nil?
    #     min_periods = window_size
    #   end
    #   _from_rbexpr(
    #     _rbexpr.rolling_apply(
    #       function, window_size, weights, min_periods, center
    #     )
    #   )
    # end

    # Compute a rolling skew.
    #
    # @param window_size [Integer]
    #   Integer size of the rolling window.
    # @param bias [Boolean]
    #   If false, the calculations are corrected for statistical bias.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 4, 2, 9]})
    #   df.select(Polars.col("a").rolling_skew(3))
    #   # =>
    #   # shape: (4, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ null     │
    #   # │ null     │
    #   # │ 0.381802 │
    #   # │ 0.47033  │
    #   # └──────────┘
    def rolling_skew(window_size, bias: true)
      _from_rbexpr(_rbexpr.rolling_skew(window_size, bias))
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
    #   # │ 0.0 │
    #   # │ 1.0 │
    #   # │ 2.0 │
    #   # └─────┘
    def abs
      _from_rbexpr(_rbexpr.abs)
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
    #   # │ 0   │
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
    # @param seed [Integer]
    #   If `method: "random"`, use this as seed.
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
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 3.0 │
    #   # │ 4.5 │
    #   # │ 1.5 │
    #   # │ 1.5 │
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
    #   # │ 4   │
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 5   │
    #   # └─────┘
    def rank(method: "average", reverse: false, seed: nil)
      _from_rbexpr(_rbexpr.rank(method, reverse, seed))
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
    #   # │ -10  │
    #   # │ 20   │
    #   # └──────┘
    def diff(n: 1, null_behavior: "ignore")
      _from_rbexpr(_rbexpr.diff(n, null_behavior))
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
    #   # │ 11   ┆ 0.1        │
    #   # │ 12   ┆ 0.090909   │
    #   # │ null ┆ 0.0        │
    #   # │ 12   ┆ 0.0        │
    #   # └──────┴────────────┘
    def pct_change(n: 1)
      n = Utils.parse_into_expression(n)
      _from_rbexpr(_rbexpr.pct_change(n))
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
      _from_rbexpr(_rbexpr.skew(bias))
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
      _from_rbexpr(_rbexpr.kurtosis(fisher, bias))
    end

    # Set values outside the given boundaries to the boundary value.
    #
    # Only works for numeric and temporal columns. If you want to clip other data
    # types, consider writing a `when-then-otherwise` expression.
    #
    # @param lower_bound [Numeric]
    #   Minimum value.
    # @param upper_bound [Numeric]
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
    #   # │ 5    ┆ 5           │
    #   # │ null ┆ null        │
    #   # │ 50   ┆ 10          │
    #   # └──────┴─────────────┘
    def clip(lower_bound = nil, upper_bound = nil)
      if !lower_bound.nil?
        lower_bound = Utils.parse_into_expression(lower_bound)
      end
      if !upper_bound.nil?
        upper_bound = Utils.parse_into_expression(upper_bound)
      end
      _from_rbexpr(_rbexpr.clip(lower_bound, upper_bound))
    end

    # Clip (limit) the values in an array to a `min` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See `when` for more information.
    #
    # @param lower_bound [Numeric]
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
    #   # │ 5    ┆ 5           │
    #   # │ null ┆ null        │
    #   # │ 50   ┆ 50          │
    #   # └──────┴─────────────┘
    def clip_min(lower_bound)
      clip(lower_bound, nil)
    end

    # Clip (limit) the values in an array to a `max` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See `when` for more information.
    #
    # @param upper_bound [Numeric]
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
    #   # │ 5    ┆ 0           │
    #   # │ null ┆ null        │
    #   # │ 50   ┆ 0           │
    #   # └──────┴─────────────┘
    def clip_max(upper_bound)
      clip(nil, upper_bound)
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
      _from_rbexpr(_rbexpr.lower_bound)
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
      _from_rbexpr(_rbexpr.upper_bound)
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
    #   # │ f64  │
    #   # ╞══════╡
    #   # │ -1.0 │
    #   # │ -0.0 │
    #   # │ 0.0  │
    #   # │ 1.0  │
    #   # │ null │
    #   # └──────┘
    def sign
      _from_rbexpr(_rbexpr.sign)
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
      _from_rbexpr(_rbexpr.sin)
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
      _from_rbexpr(_rbexpr.cos)
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
      _from_rbexpr(_rbexpr.tan)
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
      _from_rbexpr(_rbexpr.arcsin)
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
      _from_rbexpr(_rbexpr.arccos)
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
      _from_rbexpr(_rbexpr.arctan)
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
      _from_rbexpr(_rbexpr.sinh)
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
      _from_rbexpr(_rbexpr.cosh)
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
      _from_rbexpr(_rbexpr.tanh)
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
      _from_rbexpr(_rbexpr.arcsinh)
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
      _from_rbexpr(_rbexpr.arccosh)
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
      _from_rbexpr(_rbexpr.arctanh)
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
    #   square = df.select(Polars.col("foo").reshape([3, 3]))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────────────┐
    #   # │ foo           │
    #   # │ ---           │
    #   # │ array[i64, 3] │
    #   # ╞═══════════════╡
    #   # │ [1, 2, 3]     │
    #   # │ [4, 5, 6]     │
    #   # │ [7, 8, 9]     │
    #   # └───────────────┘
    #
    # @example
    #   square.select(Polars.col("foo").reshape([9]))
    #   # =>
    #   # shape: (9, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # │ 4   │
    #   # │ 5   │
    #   # │ 6   │
    #   # │ 7   │
    #   # │ 8   │
    #   # │ 9   │
    #   # └─────┘
    def reshape(dims)
      _from_rbexpr(_rbexpr.reshape(dims))
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
    #   # │ 1   │
    #   # │ 3   │
    #   # └─────┘
    def shuffle(seed: nil)
      if seed.nil?
        seed = rand(10000)
      end
      _from_rbexpr(_rbexpr.shuffle(seed))
    end

    # Sample from this expression.
    #
    # @param frac [Float]
    #   Fraction of items to return. Cannot be used with `n`.
    # @param with_replacement [Boolean]
    #   Allow values to be sampled more than once.
    # @param shuffle [Boolean]
    #   Shuffle the order of sampled data points.
    # @param seed [Integer]
    #   Seed for the random number generator. If set to None (default), a random
    #   seed is used.
    # @param n [Integer]
    #   Number of items to return. Cannot be used with `frac`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").sample(frac: 1.0, with_replacement: true, seed: 1))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 3   │
    #   # │ 1   │
    #   # │ 1   │
    #   # └─────┘
    def sample(
      frac: nil,
      with_replacement: true,
      shuffle: false,
      seed: nil,
      n: nil
    )
      if !n.nil? && !frac.nil?
        raise ArgumentError, "cannot specify both `n` and `frac`"
      end

      if !n.nil? && frac.nil?
        n = Utils.parse_into_expression(n)
        return _from_rbexpr(_rbexpr.sample_n(n, with_replacement, shuffle, seed))
      end

      if frac.nil?
        frac = 1.0
      end
      frac = Utils.parse_into_expression(frac)
      _from_rbexpr(
        _rbexpr.sample_frac(frac, with_replacement, shuffle, seed)
      )
    end

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
    #   # │ 1.666667 │
    #   # │ 2.428571 │
    #   # └──────────┘
    def ewm_mean(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      min_periods: 1,
      ignore_nulls: true
    )
      alpha = _prepare_alpha(com, span, half_life, alpha)
      _from_rbexpr(_rbexpr.ewm_mean(alpha, adjust, min_periods, ignore_nulls))
    end

    # Exponentially-weighted moving standard deviation.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").ewm_std(com: 1))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.0      │
    #   # │ 0.707107 │
    #   # │ 0.963624 │
    #   # └──────────┘
    def ewm_std(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      bias: false,
      min_periods: 1,
      ignore_nulls: true
    )
      alpha = _prepare_alpha(com, span, half_life, alpha)
      _from_rbexpr(_rbexpr.ewm_std(alpha, adjust, bias, min_periods, ignore_nulls))
    end

    # Exponentially-weighted moving variance.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    #   df.select(Polars.col("a").ewm_var(com: 1))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────────┐
    #   # │ a        │
    #   # │ ---      │
    #   # │ f64      │
    #   # ╞══════════╡
    #   # │ 0.0      │
    #   # │ 0.5      │
    #   # │ 0.928571 │
    #   # └──────────┘
    def ewm_var(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      bias: false,
      min_periods: 1,
      ignore_nulls: true
    )
      alpha = _prepare_alpha(com, span, half_life, alpha)
      _from_rbexpr(_rbexpr.ewm_var(alpha, adjust, bias, min_periods, ignore_nulls))
    end

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
    #   # │ 2      │
    #   # │ 3      │
    #   # │ 99     │
    #   # │ 99     │
    #   # └────────┘
    def extend_constant(value, n)
      _from_rbexpr(_rbexpr.extend_constant(value, n))
    end

    # Count all unique values and create a struct mapping value to count.
    #
    # @param sort [Boolean]
    #   Sort the output by count in descending order.
    #   If set to `false` (default), the order of the output is random.
    # @param parallel [Boolean]
    #   Execute the computation in parallel.
    # @param name [String]
    #   Give the resulting count column a specific name;
    #   if `normalize` is true defaults to "count",
    #   otherwise defaults to "proportion".
    # @param normalize [Boolean]
    #   If true gives relative frequencies of the unique values
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
    #   # │ {"b",2}   │
    #   # │ {"a",1}   │
    #   # └───────────┘
    def value_counts(
      sort: false,
      parallel: false,
      name: nil,
      normalize: false
    )
      if name.nil?
        if normalize
          name = "proportion"
        else
          name = "count"
        end
      end
      _from_rbexpr(
        _rbexpr.value_counts(sort, parallel, name, normalize)
      )
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
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    def unique_counts
      _from_rbexpr(_rbexpr.unique_counts)
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
    #   # │ 1.0      │
    #   # │ 1.584963 │
    #   # └──────────┘
    def log(base = Math::E)
      _from_rbexpr(_rbexpr.log(base))
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
      _from_rbexpr(_rbexpr.entropy(base, normalize))
    end

    # Run an expression over a sliding window that increases `1` slot every iteration.
    #
    # @param expr [Expr]
    #   Expression to evaluate
    # @param min_periods [Integer]
    #   Number of valid values there should be in the window before the expression
    #   is evaluated. valid values = `length - null_count`
    # @param parallel [Boolean]
    #   Run in parallel. Don't do this in a group by or another operation that
    #   already has much parallelization.
    #
    # @return [Expr]
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   This can be really slow as it can have `O(n^2)` complexity. Don't use this
    #   for operations that visit all elements.
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [1, 2, 3, 4, 5]})
    #   df.select(
    #     [
    #       Polars.col("values").cumulative_eval(
    #         Polars.element.first - Polars.element.last ** 2
    #       )
    #     ]
    #   )
    #   # =>
    #   # shape: (5, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 0      │
    #   # │ -3     │
    #   # │ -8     │
    #   # │ -15    │
    #   # │ -24    │
    #   # └────────┘
    def cumulative_eval(expr, min_periods: 1, parallel: false)
      _from_rbexpr(
        _rbexpr.cumulative_eval(expr._rbexpr, min_periods, parallel)
      )
    end

    # Flags the expression as 'sorted'.
    #
    # Enables downstream code to user fast paths for sorted arrays.
    #
    # @param descending [Boolean]
    #   Whether the `Series` order is descending.
    #
    # @return [Expr]
    #
    # @note
    #   This can lead to incorrect results if this `Series` is not sorted!!
    #   Use with care!
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => [1, 2, 3]})
    #   df.select(Polars.col("values").set_sorted.max)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌────────┐
    #   # │ values │
    #   # │ ---    │
    #   # │ i64    │
    #   # ╞════════╡
    #   # │ 3      │
    #   # └────────┘
    def set_sorted(descending: false)
      _from_rbexpr(_rbexpr.set_sorted_flag(descending))
    end

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
    #   df.select(Polars.all.implode)
    #   # =>
    #   # shape: (1, 2)
    #   # ┌───────────┬───────────┐
    #   # │ a         ┆ b         │
    #   # │ ---       ┆ ---       │
    #   # │ list[i64] ┆ list[i64] │
    #   # ╞═══════════╪═══════════╡
    #   # │ [1, 2, 3] ┆ [4, 5, 6] │
    #   # └───────────┴───────────┘
    def implode
      _from_rbexpr(_rbexpr.implode)
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
    #   # │ 2   ┆ 2          ┆ 2          ┆ 2    ┆ 2    ┆ b   ┆ 1.32 ┆ null  │
    #   # │ 3   ┆ 8589934592 ┆ 1073741824 ┆ 112  ┆ 129  ┆ c   ┆ 0.12 ┆ false │
    #   # └─────┴────────────┴────────────┴──────┴──────┴─────┴──────┴───────┘
    def shrink_dtype
      _from_rbexpr(_rbexpr.shrink_dtype)
    end

    # Replace values by different values.
    #
    # @param old [Object]
    #   Value or sequence of values to replace.
    #   Accepts expression input. Sequences are parsed as Series,
    #   other non-expression inputs are parsed as literals.
    #   Also accepts a mapping of values to their replacement.
    # @param new [Object]
    #   Value or sequence of values to replace by.
    #   Accepts expression input. Sequences are parsed as Series,
    #   other non-expression inputs are parsed as literals.
    #   Length must match the length of `old` or have length 1.
    # @param default [Object]
    #   Set values that were not replaced to this value.
    #   Defaults to keeping the original value.
    #   Accepts expression input. Non-expression inputs are parsed as literals.
    # @param return_dtype [Object]
    #   The data type of the resulting expression. If set to `nil` (default),
    #   the data type is determined automatically based on the other inputs.
    #
    # @return [Expr]
    #
    # @example Replace a single value by another value. Values that were not replaced remain unchanged.
    #   df = Polars::DataFrame.new({"a" => [1, 2, 2, 3]})
    #   df.with_columns(replaced: Polars.col("a").replace(2, 100))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ i64 ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ 1   ┆ 1        │
    #   # │ 2   ┆ 100      │
    #   # │ 2   ┆ 100      │
    #   # │ 3   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example Replace multiple values by passing sequences to the `old` and `new` parameters.
    #   df.with_columns(replaced: Polars.col("a").replace([2, 3], [100, 200]))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ i64 ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ 1   ┆ 1        │
    #   # │ 2   ┆ 100      │
    #   # │ 2   ┆ 100      │
    #   # │ 3   ┆ 200      │
    #   # └─────┴──────────┘
    #
    # @example Passing a mapping with replacements is also supported as syntactic sugar. Specify a default to set all values that were not matched.
    #   mapping = {2 => 100, 3 => 200}
    #   df.with_columns(replaced: Polars.col("a").replace(mapping, default: -1))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ i64 ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ 1   ┆ -1       │
    #   # │ 2   ┆ 100      │
    #   # │ 2   ┆ 100      │
    #   # │ 3   ┆ 200      │
    #   # └─────┴──────────┘
    #
    # @example Replacing by values of a different data type sets the return type based on a combination of the `new` data type and either the original data type or the default data type if it was set.
    #   df = Polars::DataFrame.new({"a" => ["x", "y", "z"]})
    #   mapping = {"x" => 1, "y" => 2, "z" => 3}
    #   df.with_columns(replaced: Polars.col("a").replace(mapping))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ str ┆ str      │
    #   # ╞═════╪══════════╡
    #   # │ x   ┆ 1        │
    #   # │ y   ┆ 2        │
    #   # │ z   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example
    #   df.with_columns(replaced: Polars.col("a").replace(mapping, default: nil))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ str ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ x   ┆ 1        │
    #   # │ y   ┆ 2        │
    #   # │ z   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example Set the `return_dtype` parameter to control the resulting data type directly.
    #   df.with_columns(
    #     replaced: Polars.col("a").replace(mapping, return_dtype: Polars::UInt8)
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ str ┆ u8       │
    #   # ╞═════╪══════════╡
    #   # │ x   ┆ 1        │
    #   # │ y   ┆ 2        │
    #   # │ z   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example Expression input is supported for all parameters.
    #   df = Polars::DataFrame.new({"a" => [1, 2, 2, 3], "b" => [1.5, 2.5, 5.0, 1.0]})
    #   df.with_columns(
    #     replaced: Polars.col("a").replace(
    #       Polars.col("a").max,
    #       Polars.col("b").sum,
    #       default: Polars.col("b")
    #     )
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬──────────┐
    #   # │ a   ┆ b   ┆ replaced │
    #   # │ --- ┆ --- ┆ ---      │
    #   # │ i64 ┆ f64 ┆ f64      │
    #   # ╞═════╪═════╪══════════╡
    #   # │ 1   ┆ 1.5 ┆ 1.5      │
    #   # │ 2   ┆ 2.5 ┆ 2.5      │
    #   # │ 2   ┆ 5.0 ┆ 5.0      │
    #   # │ 3   ┆ 1.0 ┆ 10.0     │
    #   # └─────┴─────┴──────────┘
    def replace(old, new = NO_DEFAULT, default: NO_DEFAULT, return_dtype: nil)
      if !default.eql?(NO_DEFAULT)
        return replace_strict(old, new, default: default, return_dtype: return_dtype)
      end

      if new.eql?(NO_DEFAULT) && old.is_a?(Hash)
        new = Series.new(old.values)
        old = Series.new(old.keys)
      else
        if old.is_a?(::Array)
          old = Series.new(old)
        end
        if new.is_a?(::Array)
          new = Series.new(new)
        end
      end

      old = Utils.parse_into_expression(old, str_as_lit: true)
      new = Utils.parse_into_expression(new, str_as_lit: true)

      result = _from_rbexpr(_rbexpr.replace(old, new))

      if !return_dtype.nil?
        result = result.cast(return_dtype)
      end

      result
    end

    # Replace all values by different values.
    #
    # @param old [Object]
    #   Value or sequence of values to replace.
    #   Accepts expression input. Sequences are parsed as Series,
    #   other non-expression inputs are parsed as literals.
    #   Also accepts a mapping of values to their replacement as syntactic sugar for
    #   `replace_all(old: Series.new(mapping.keys), new: Serie.new(mapping.values))`.
    # @param new [Object]
    #   Value or sequence of values to replace by.
    #   Accepts expression input. Sequences are parsed as Series,
    #   other non-expression inputs are parsed as literals.
    #   Length must match the length of `old` or have length 1.
    # @param default [Object]
    #   Set values that were not replaced to this value. If no default is specified,
    #   (default), an error is raised if any values were not replaced.
    #   Accepts expression input. Non-expression inputs are parsed as literals.
    # @param return_dtype [Object]
    #   The data type of the resulting expression. If set to `nil` (default),
    #   the data type is determined automatically based on the other inputs.
    #
    # @return [Expr]
    #
    # @note
    #   The global string cache must be enabled when replacing categorical values.
    #
    # @example Replace values by passing sequences to the `old` and `new` parameters.
    #   df = Polars::DataFrame.new({"a" => [1, 2, 2, 3]})
    #   df.with_columns(
    #     replaced: Polars.col("a").replace_strict([1, 2, 3], [100, 200, 300])
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ i64 ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ 1   ┆ 100      │
    #   # │ 2   ┆ 200      │
    #   # │ 2   ┆ 200      │
    #   # │ 3   ┆ 300      │
    #   # └─────┴──────────┘
    #
    # @example By default, an error is raised if any non-null values were not replaced. Specify a default to set all values that were not matched.
    #   mapping = {2 => 200, 3 => 300}
    #   df.with_columns(replaced: Polars.col("a").replace_strict(mapping, default: -1))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ i64 ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ 1   ┆ -1       │
    #   # │ 2   ┆ 200      │
    #   # │ 2   ┆ 200      │
    #   # │ 3   ┆ 300      │
    #   # └─────┴──────────┘
    #
    # @example Replacing by values of a different data type sets the return type based on a combination of the `new` data type and the `default` data type.
    #   df = Polars::DataFrame.new({"a" => ["x", "y", "z"]})
    #   mapping = {"x" => 1, "y" => 2, "z" => 3}
    #   df.with_columns(replaced: Polars.col("a").replace_strict(mapping))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ str ┆ i64      │
    #   # ╞═════╪══════════╡
    #   # │ x   ┆ 1        │
    #   # │ y   ┆ 2        │
    #   # │ z   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example
    #   df.with_columns(replaced: Polars.col("a").replace_strict(mapping, default: "x"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ str ┆ str      │
    #   # ╞═════╪══════════╡
    #   # │ x   ┆ 1        │
    #   # │ y   ┆ 2        │
    #   # │ z   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example Set the `return_dtype` parameter to control the resulting data type directly.
    #   df.with_columns(
    #     replaced: Polars.col("a").replace_strict(mapping, return_dtype: Polars::UInt8)
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬──────────┐
    #   # │ a   ┆ replaced │
    #   # │ --- ┆ ---      │
    #   # │ str ┆ u8       │
    #   # ╞═════╪══════════╡
    #   # │ x   ┆ 1        │
    #   # │ y   ┆ 2        │
    #   # │ z   ┆ 3        │
    #   # └─────┴──────────┘
    #
    # @example Expression input is supported for all parameters.
    #   df = Polars::DataFrame.new({"a" => [1, 2, 2, 3], "b" => [1.5, 2.5, 5.0, 1.0]})
    #   df.with_columns(
    #     replaced: Polars.col("a").replace_strict(
    #       Polars.col("a").max,
    #       Polars.col("b").sum,
    #       default: Polars.col("b")
    #     )
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬──────────┐
    #   # │ a   ┆ b   ┆ replaced │
    #   # │ --- ┆ --- ┆ ---      │
    #   # │ i64 ┆ f64 ┆ f64      │
    #   # ╞═════╪═════╪══════════╡
    #   # │ 1   ┆ 1.5 ┆ 1.5      │
    #   # │ 2   ┆ 2.5 ┆ 2.5      │
    #   # │ 2   ┆ 5.0 ┆ 5.0      │
    #   # │ 3   ┆ 1.0 ┆ 10.0     │
    #   # └─────┴─────┴──────────┘
    def replace_strict(
      old,
      new = NO_DEFAULT,
      default: NO_DEFAULT,
      return_dtype: nil
    )
      if new.eql?(NO_DEFAULT) && old.is_a?(Hash)
        new = Series.new(old.values)
        old = Series.new(old.keys)
      end

      old = Utils.parse_into_expression(old, str_as_lit: true, list_as_series: true)
      new = Utils.parse_into_expression(new, str_as_lit: true, list_as_series: true)

      default = default.eql?(NO_DEFAULT) ? nil : Utils.parse_into_expression(default, str_as_lit: true)

      _from_rbexpr(
        _rbexpr.replace_strict(old, new, default, return_dtype)
      )
    end

    # Create an object namespace of all list related methods.
    #
    # @return [ListExpr]
    def list
      ListExpr.new(self)
    end

    # Create an object namespace of all array related methods.
    #
    # @return [ArrayExpr]
    def arr
      ArrayExpr.new(self)
    end

    # Create an object namespace of all binary related methods.
    #
    # @return [BinaryExpr]
    def bin
      BinaryExpr.new(self)
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

    # Create an object namespace of all expressions that modify expression names.
    #
    # @return [NameExpr]
    def name
      NameExpr.new(self)
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

    def _from_rbexpr(expr)
      Utils.wrap_expr(expr)
    end

    def _to_rbexpr(other)
      _to_expr(other)._rbexpr
    end

    def _to_expr(other)
      other.is_a?(Expr) ? other : F.lit(other)
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

    def _prepare_rolling_by_window_args(window_size)
      window_size
    end
  end
end

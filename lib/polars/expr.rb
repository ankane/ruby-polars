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

    def alias(name)
      wrap_expr(_rbexpr._alias(name))
    end

    # TODO support symbols for exclude

    #
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

    def keep_name
      wrap_expr(_rbexpr.keep_name)
    end

    def prefix(prefix)
      wrap_expr(_rbexpr.prefix(prefix))
    end

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

    def is_nan
      wrap_expr(_rbexpr.is_nan)
    end

    def is_not_nan
      wrap_expr(_rbexpr.is_not_nan)
    end

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

    def append(other, upcast: true)
      other = Utils.expr_to_lit_or_expr(other)
      wrap_expr(_rbexpr.append(other._rbexpr, upcast))
    end

    # Create a single chunk of memory for this Series.
    #
    # @return [Expr]
    #
    # @example Create a Series with 3 nulls, append column a then rechunk
    #   df = Polars::DataFrame.new({"a": [1, 1, 2]})
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

    def cumsum(reverse: false)
      wrap_expr(_rbexpr.cumsum(reverse))
    end

    def cumprod(reverse: false)
      wrap_expr(_rbexpr.cumprod(reverse))
    end

    def cummin(reverse: false)
      wrap_expr(_rbexpr.cummin(reverse))
    end

    def cummax(reverse: false)
      wrap_expr(_rbexpr.cummax(reverse))
    end

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

    def cast(dtype, strict: true)
      dtype = Utils.rb_type_to_dtype(dtype)
      wrap_expr(_rbexpr.cast(dtype, strict))
    end

    def sort(reverse: false, nulls_last: false)
      wrap_expr(_rbexpr.sort_with(reverse, nulls_last))
    end

    def top_k(k: 5, reverse: false)
      wrap_expr(_rbexpr.top_k(k, reverse))
    end

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

    def search_sorted(element)
      element = Utils.expr_to_lit_or_expr(element, str_to_lit: false)
      wrap_expr(_rbexpr.search_sorted(element._rbexpr))
    end

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

    def shift_and_fill(periods, fill_value)
      fill_value = Utils.expr_to_lit_or_expr(fill_value, str_to_lit: true)
      wrap_expr(_rbexpr.shift_and_fill(periods, fill_value._rbexpr))
    end

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

    def fill_nan(fill_value)
      fill_value = Utils.expr_to_lit_or_expr(fill_value, str_to_lit: true)
      wrap_expr(_rbexpr.fill_nan(fill_value._rbexpr))
    end

    def forward_fill(limit: nil)
      wrap_expr(_rbexpr.forward_fill(limit))
    end

    def backward_fill(limit: nil)
      wrap_expr(_rbexpr.backward_fill(limit))
    end

    def reverse
      wrap_expr(_rbexpr.reverse)
    end

    def std(ddof: 1)
      wrap_expr(_rbexpr.std(ddof))
    end

    def var(ddof: 1)
      wrap_expr(_rbexpr.var(ddof))
    end

    def max
      wrap_expr(_rbexpr.max)
    end

    def min
      wrap_expr(_rbexpr.min)
    end

    def nan_max
      wrap_expr(_rbexpr.nan_max)
    end

    def nan_min
      wrap_expr(_rbexpr.nan_min)
    end

    def sum
      wrap_expr(_rbexpr.sum)
    end

    def mean
      wrap_expr(_rbexpr.mean)
    end

    def median
      wrap_expr(_rbexpr.median)
    end

    def product
      wrap_expr(_rbexpr.product)
    end

    def n_unique
      wrap_expr(_rbexpr.n_unique)
    end

    def null_count
      wrap_expr(_rbexpr.null_count)
    end

    def arg_unique
      wrap_expr(_rbexpr.arg_unique)
    end

    def unique(maintain_order: false)
      if maintain_order
        wrap_expr(_rbexpr.unique_stable)
      else
        wrap_expr(_rbexpr.unique)
      end
    end

    def first
      wrap_expr(_rbexpr.first)
    end

    def last
      wrap_expr(_rbexpr.last)
    end

    def over(expr)
      rbexprs = Utils.selection_to_rbexpr_list(expr)
      wrap_expr(_rbexpr.over(rbexprs))
    end

    def is_unique
      wrap_expr(_rbexpr.is_unique)
    end

    def is_first
      wrap_expr(_rbexpr.is_first)
    end

    def is_duplicated
      wrap_expr(_rbexpr.is_duplicated)
    end

    def quantile(quantile, interpolation: "nearest")
      wrap_expr(_rbexpr.quantile(quantile, interpolation))
    end

    def filter(predicate)
      wrap_expr(_rbexpr.filter(predicate._rbexpr))
    end

    def where(predicate)
      filter(predicate)
    end

    # def map
    # end

    # def apply
    # end

    #
    def flatten
      wrap_expr(_rbexpr.explode)
    end

    def explode
      wrap_expr(_rbexpr.explode)
    end

    def take_every(n)
      wrap_expr(_rbexpr.take_every(n))
    end

    def head(n = 10)
      wrap_expr(_rbexpr.head(n))
    end

    def tail(n = 10)
      wrap_expr(_rbexpr.tail(n))
    end

    def limit(n = 10)
      head(n)
    end

    def pow(exponent)
      exponent = Utils.expr_to_lit_or_expr(exponent)
      wrap_expr(_rbexpr.pow(exponent._rbexpr))
    end

    # def is_in
    # end

    #
    def repeat_by(by)
      by = Utils.expr_to_lit_or_expr(by, false)
      wrap_expr(_rbexpr.repeat_by(by._rbexpr))
    end

    # def is_between
    # end

    # def _hash
    # end

    #
    def reinterpret(signed: false)
      wrap_expr(_rbexpr.reinterpret(signed))
    end

    # def _inspect
    # end

    #
    def interpolate
      wrap_expr(_rbexpr.interpolate)
    end

    # def rolling_min
    # end

    # def rolling_max
    # end

    # def rolling_mean
    # end

    # def rolling_sum
    # end

    # def rolling_std
    # end

    # def rolling_var
    # end

    # def rolling_median
    # end

    # def rolling_quantile
    # end

    # def rolling_apply
    # end

    #
    def rolling_skew(window_size, bias: true)
      wrap_expr(_rbexpr.rolling_skew(window_size, bias))
    end

    def abs
      wrap_expr(_rbexpr.abs)
    end

    def argsort(reverse: false, nulls_last: false)
      arg_sort(reverse: reverse, nulls_last: nulls_last)
    end

    def rank(method: "average", reverse: false)
      wrap_expr(_rbexpr.rank(method, reverse))
    end

    def diff(n: 1, null_behavior: "ignore")
      wrap_expr(_rbexpr.diff(n, null_behavior))
    end

    def pct_change(n: 1)
      wrap_expr(_rbexpr.pct_change(n))
    end

    def skew(bias: true)
      wrap_expr(_rbexpr.skew(bias))
    end

    def kurtosis(fisher: true, bias: true)
      wrap_expr(_rbexpr.kurtosis(fisher, bias))
    end

    def clip(min_val, max_val)
      wrap_expr(_rbexpr.clip(min_val, max_val))
    end

    def clip_min(min_val)
      wrap_expr(_rbexpr.clip_min(min_val))
    end

    def clip_max(max_val)
      wrap_expr(_rbexpr.clip_max(max_val))
    end

    def lower_bound
      wrap_expr(_rbexpr.lower_bound)
    end

    def upper_bound
      wrap_expr(_rbexpr.upper_bound)
    end

    def sign
      wrap_expr(_rbexpr.sign)
    end

    def sin
      wrap_expr(_rbexpr.sin)
    end

    def cos
      wrap_expr(_rbexpr.cos)
    end

    def tan
      wrap_expr(_rbexpr.tan)
    end

    def arcsin
      wrap_expr(_rbexpr.arcsin)
    end

    def arccos
      wrap_expr(_rbexpr.arccos)
    end

    def arctan
      wrap_expr(_rbexpr.arctan)
    end

    def sinh
      wrap_expr(_rbexpr.sinh)
    end

    def cosh
      wrap_expr(_rbexpr.cosh)
    end

    def tanh
      wrap_expr(_rbexpr.tanh)
    end

    def arcsinh
      wrap_expr(_rbexpr.arcsinh)
    end

    def arccosh
      wrap_expr(_rbexpr.arccosh)
    end

    def arctanh
      wrap_expr(_rbexpr.arctanh)
    end

    def reshape(dims)
      wrap_expr(_rbexpr.reshape(dims))
    end

    def shuffle(seed: nil)
      if seed.nil?
        seed = rand(10000)
      end
      wrap_expr(_rbexpr.shuffle(seed))
    end

    # def sample
    # end

    # def ewm_mean
    # end

    # def ewm_std
    # end

    # def ewm_var
    # end

    #
    def extend_constant(value, n)
      wrap_expr(_rbexpr.extend_constant(value, n))
    end

    def value_counts(multithreaded: false, sort: false)
      wrap_expr(_rbexpr.value_counts(multithreaded, sort))
    end

    def unique_counts
      wrap_expr(_rbexpr.unique_counts)
    end

    def log(base = Math::E)
      wrap_expr(_rbexpr.log(base))
    end

    def entropy(base: 2, normalize: false)
      wrap_expr(_rbexpr.entropy(base, normalize))
    end

    # def cumulative_eval
    # end

    # def set_sorted
    # end

    #
    def list
      wrap_expr(_rbexpr.list)
    end

    def shrink_dtype
      wrap_expr(_rbexpr.shrink_dtype)
    end

    def arr
      ListExpr.new(self)
    end

    def cat
      CatExpr.new(self)
    end

    def dt
      DateTimeExpr.new(self)
    end

    def meta
      MetaExpr.new(self)
    end

    def str
      StringExpr.new(self)
    end

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
  end
end

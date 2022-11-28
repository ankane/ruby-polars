module Polars
  module LazyFunctions
    # Return an expression representing a column in a DataFrame.
    #
    # @return [Expr]
    def col(name)
      if name.is_a?(Series)
        name = name.to_a
      end

      if name.is_a?(Array)
        if name.length == 0 || name[0].is_a?(String) || name[0].is_a?(Symbol)
          name = name.map { |v| v.is_a?(Symbol) ? v.to_s : v }
          Utils.wrap_expr(RbExpr.cols(name))
        elsif Utils.is_polars_dtype(name[0])
          raise Todo
          # Utils.wrap_expr(_dtype_cols(name))
        else
          raise ArgumentError, "Expected list values to be all `str` or all `DataType`"
        end
      else
        name = name.to_s if name.is_a?(Symbol)
        Utils.wrap_expr(RbExpr.col(name))
      end
    end

    # Alias for an element in evaluated in an `eval` expression.
    #
    # @return [Expr]
    #
    # @example A horizontal rank computation by taking the elements of a list
    #   df = Polars::DataFrame.new({"a" => [1, 8, 3], "b" => [4, 5, 2]})
    #   df.with_column(
    #     Polars.concat_list(["a", "b"]).arr.eval(Polars.element.rank).alias("rank")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬────────────┐
    #   # │ a   ┆ b   ┆ rank       │
    #   # │ --- ┆ --- ┆ ---        │
    #   # │ i64 ┆ i64 ┆ list[f32]  │
    #   # ╞═════╪═════╪════════════╡
    #   # │ 1   ┆ 4   ┆ [1.0, 2.0] │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 8   ┆ 5   ┆ [2.0, 1.0] │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 2   ┆ [2.0, 1.0] │
    #   # └─────┴─────┴────────────┘
    def element
      col("")
    end

    # Count the number of values in this column/context.
    #
    # @param column [String, Series, nil]
    #     If dtype is:
    #
    #     * `Series` : count the values in the series.
    #     * `String` : count the values in this column.
    #     * `None` : count the number of values in this context.
    #
    # @return [Expr, Integer]
    def count(column = nil)
      if column.nil?
        return Utils.wrap_expr(RbExpr.count)
      end

      if column.is_a?(Series)
        column.len
      else
        col(column).count
      end
    end

    # Aggregate to list.
    #
    # @return [Expr]
    def to_list(name)
      col(name).list
    end

    # Get the standard deviation.
    #
    # @return [Object]
    def std(column, ddof: 1)
      if column.is_a?(Series)
        column.std(ddof: ddof)
      else
        col(column).std(ddof: ddof)
      end
    end

    # Get the variance.
    #
    # @return [Object]
    def var(column, ddof: 1)
      if column.is_a?(Series)
        column.var(ddof: ddof)
      else
        col(column).var(ddof: ddof)
      end
    end

    # Get the maximum value.
    #
    # @param column [Object]
    #   Column(s) to be used in aggregation. Will lead to different behavior based on
    #   the input:
    #
    #   - [String, Series] -> aggregate the maximum value of that column.
    #   - [Array<Expr>] -> aggregate the maximum value horizontally.
    #
    # @return [Expr, Object]
    def max(column)
      if column.is_a?(Series)
        column.max
      elsif column.is_a?(String) || column.is_a?(Symbol)
        col(column).max
      else
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_max_exprs(exprs))
      end
    end

    # Get the minimum value.
    #
    # @param column [Object]
    #   Column(s) to be used in aggregation. Will lead to different behavior based on
    #   the input:
    #
    #   - [String, Series] -> aggregate the minimum value of that column.
    #   - [Array<Expr>] -> aggregate the minimum value horizontally.
    #
    # @return [Expr, Object]
    def min(column)
      if column.is_a?(Series)
        column.min
      elsif column.is_a?(String) || column.is_a?(Symbol)
        col(column).min
      else
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_min_exprs(exprs))
      end
    end

    # Sum values in a column/Series, or horizontally across list of columns/expressions.
    #
    # @return [Object]
    def sum(column)
      if column.is_a?(Series)
        column.sum
      elsif column.is_a?(String) || column.is_a?(Symbol)
        col(column.to_s).sum
      elsif column.is_a?(Array)
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_sum_exprs(exprs))
      else
        raise Todo
      end
    end

    # Get the mean value.
    #
    # @return [Expr, Float]
    def mean(column)
      if column.is_a?(Series)
        column.mean
      else
        col(column).mean
      end
    end

    # Get the mean value.
    #
    # @return [Expr, Float]
    def avg(column)
      mean(column)
    end

    # Get the median value.
    #
    # @return [Object]
    def median(column)
      if column.is_a?(Series)
        column.median
      else
        col(column).median
      end
    end

    # def n_unique
    # end

    # Get the first value.
    #
    # @return [Object]
    def first(column = nil)
      if column.nil?
        return Utils.wrap_expr(RbExpr.first)
      end

      if column.is_a?(Series)
        if column.len > 0
          column[0]
        else
          raise IndexError, "The series is empty, so no first value can be returned."
        end
      else
        col(column).first
      end
    end

    # def last
    # end

    # def head
    # end

    # def tail
    # end

    # Return an expression representing a literal value.
    #
    # @return [Expr]
    def lit(value)
      if value.is_a?(Polars::Series)
        name = value.name
        value = value._s
        e = Utils.wrap_expr(RbExpr.lit(value))
        if name == ""
          return e
        end
        return e.alias(name)
      end

      Utils.wrap_expr(RbExpr.lit(value))
    end

    # def cumsum
    # end

    # def spearman_rank_corr
    # end

    # def pearson_corr
    # end

    # def cov
    # end

    # def map
    # end

    # def apply
    # end

    # Accumulate over multiple columns horizontally/ row wise with a left fold.
    #
    # @return [Expr]
    def fold(acc, f, exprs)
      acc = Utils.expr_to_lit_or_expr(acc, str_to_lit: true)
      if exprs.is_a?(Expr)
        exprs = [exprs]
      end

      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.fold(acc._rbexpr, f, exprs))
    end

    # def reduce
    # end

    # def cumfold
    # end

    # def cumreduce
    # end

    # def any
    # end

    # def exclude
    # end

    # Do one of two things.
    #
    # * function can do a columnwise or elementwise AND operation
    # * a wildcard column selection
    #
    # @param name [Object]
    #   If given this function will apply a bitwise & on the columns.
    #
    # @return [Expr]
    #
    # @example Sum all columns
    #   df = Polars::DataFrame.new(
    #     {"a" => [1, 2, 3], "b" => ["hello", "foo", "bar"], "c" => [1, 1, 1]}
    #   )
    #   df.select(Polars.all.sum)
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬──────┬─────┐
    #   # │ a   ┆ b    ┆ c   │
    #   # │ --- ┆ ---  ┆ --- │
    #   # │ i64 ┆ str  ┆ i64 │
    #   # ╞═════╪══════╪═════╡
    #   # │ 6   ┆ null ┆ 3   │
    #   # └─────┴──────┴─────┘
    def all(name = nil)
      if name.nil?
        col("*")
      elsif name.is_a?(String) || name.is_a?(Symbol)
        col(name).all
      else
        raise Todo
      end
    end

    # def groups
    # end

    # def quantile
    # end

    # Create a range expression (or Series).
    #
    # This can be used in a `select`, `with_column`, etc. Be sure that the resulting
    # range size is equal to the length of the DataFrame you are collecting.
    #
    # @param low [Integer, Expr, Series]
    #   Lower bound of range.
    # @param high [Integer, Expr, Series]
    #   Upper bound of range.
    # @param step [Integer]
    #   Step size of the range.
    # @param eager [Boolean]
    #   If eager evaluation is `True`, a Series is returned instead of an Expr.
    # @param dtype [Symbol]
    #   Apply an explicit integer dtype to the resulting expression (default is Int64).
    #
    # @return [Expr, Series]
    #
    # @example
    #   df.lazy.filter(Polars.col("foo") < Polars.arange(0, 100)).collect
    def arange(low, high, step: 1, eager: false, dtype: nil)
      low = Utils.expr_to_lit_or_expr(low, str_to_lit: false)
      high = Utils.expr_to_lit_or_expr(high, str_to_lit: false)
      range_expr = Utils.wrap_expr(RbExpr.arange(low._rbexpr, high._rbexpr, step))

      if !dtype.nil? && dtype != "i64"
        range_expr = range_expr.cast(dtype)
      end

      if !eager
        range_expr
      else
        DataFrame.new
          .select(range_expr)
          .to_series
          .rename("arange", in_place: true)
      end
    end

    # def argsort_by
    # end

    # def duration
    # end

    # def format
    # end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @return [Expr]
    def concat_list(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.concat_lst(exprs))
    end

    # def collect_all
    # end

    # Run polars expressions without a context.
    #
    # @return [DataFrame]
    def select(exprs)
      DataFrame.new([]).select(exprs)
    end

    # Collect several columns into a Series of dtype Struct.
    #
    # @param exprs [Object]
    #   Columns/Expressions to collect into a Struct
    # @param eager [Boolean]
    #   Evaluate immediately
    #
    # @return [Object]
    #
    # @example
    #   Polars::DataFrame.new(
    #     {
    #       "int" => [1, 2],
    #       "str" => ["a", "b"],
    #       "bool" => [true, nil],
    #       "list" => [[1, 2], [3]],
    #     }
    #   ).select([Polars.struct(Polars.all()).alias("my_struct")])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ my_struct           │
    #   # │ ---                 │
    #   # │ struct[4]           │
    #   # ╞═════════════════════╡
    #   # │ {1,"a",true,[1, 2]} │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ {2,"b",null,[3]}    │
    #   # └─────────────────────┘
    #
    # @example Only collect specific columns as a struct:
    #   df = Polars::DataFrame.new(
    #     {"a" => [1, 2, 3, 4], "b" => ["one", "two", "three", "four"], "c" => [9, 8, 7, 6]}
    #   )
    #   df.with_column(pl.struct(pl.col(["a", "b"])).alias("a_and_b"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬───────┬─────┬─────────────┐
    #   # │ a   ┆ b     ┆ c   ┆ a_and_b     │
    #   # │ --- ┆ ---   ┆ --- ┆ ---         │
    #   # │ i64 ┆ str   ┆ i64 ┆ struct[2]   │
    #   # ╞═════╪═══════╪═════╪═════════════╡
    #   # │ 1   ┆ one   ┆ 9   ┆ {1,"one"}   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ two   ┆ 8   ┆ {2,"two"}   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ three ┆ 7   ┆ {3,"three"} │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 4   ┆ four  ┆ 6   ┆ {4,"four"}  │
    #   # └─────┴───────┴─────┴─────────────┘
    def struct(exprs, eager: false)
      if eager
        Polars.select(struct(exprs, eager: false)).to_series
      end
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(_as_struct(exprs))
    end

    # def repeat
    # end

    # Return indices where `condition` evaluates `true`.
    #
    # @param condition [Expr]
    #   Boolean expression to evaluate
    # @param eager [Boolean]
    #   Whether to apply this function eagerly (as opposed to lazily).
    #
    # @return [Expr, Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4, 5]})
    #   df.select(
    #     [
    #       Polars.arg_where(Polars.col("a") % 2 == 0)
    #     ]
    #   ).to_series
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def arg_where(condition, eager: false)
      if eager
        if !condition.is_a?(Series)
          raise ArgumentError, "expected 'Series' in 'arg_where' if 'eager=True', got #{condition.class.name}"
        end
        condition.to_frame.select(arg_where(Polars.col(condition.name))).to_series
      else
        condition = Utils.expr_to_lit_or_expr(condition, str_to_lit: true)
        Utils.wrap_expr(_arg_where(condition._rbexpr))
      end
    end

    # def coalesce
    # end

    # def from_epoch
    # end

    # Start a "when, then, otherwise" expression.
    #
    # @return [When]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 3, 4], "bar" => [3, 4, 0]})
    #   df.with_column(Polars.when(Polars.col("foo") > 2).then(Polars.lit(1)).otherwise(Polars.lit(-1)))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────────┐
    #   # │ foo ┆ bar ┆ literal │
    #   # │ --- ┆ --- ┆ ---     │
    #   # │ i64 ┆ i64 ┆ i32     │
    #   # ╞═════╪═════╪═════════╡
    #   # │ 1   ┆ 3   ┆ -1      │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 4   ┆ 1       │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ 4   ┆ 0   ┆ 1       │
    #   # └─────┴─────┴─────────┘
    def when(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      pw = RbExpr.when(expr._rbexpr)
      When.new(pw)
    end
  end
end

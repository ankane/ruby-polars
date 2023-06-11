module Polars
  module LazyFunctions
    # Return an expression representing a column in a DataFrame.
    #
    # @return [Expr]
    def col(name)
      if name.is_a?(Series)
        name = name.to_a
      end

      if name.is_a?(Class) && name < DataType
        name = [name]
      end

      if name.is_a?(DataType)
        Utils.wrap_expr(_dtype_cols([name]))
      elsif name.is_a?(::Array)
        if name.length == 0 || Utils.strlike?(name[0])
          name = name.map { |v| v.is_a?(Symbol) ? v.to_s : v }
          Utils.wrap_expr(RbExpr.cols(name))
        elsif Utils.is_polars_dtype(name[0])
          Utils.wrap_expr(_dtype_cols(name))
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
    #     Polars.concat_list(["a", "b"]).list.eval(Polars.element.rank).alias("rank")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬────────────┐
    #   # │ a   ┆ b   ┆ rank       │
    #   # │ --- ┆ --- ┆ ---        │
    #   # │ i64 ┆ i64 ┆ list[f32]  │
    #   # ╞═════╪═════╪════════════╡
    #   # │ 1   ┆ 4   ┆ [1.0, 2.0] │
    #   # │ 8   ┆ 5   ┆ [2.0, 1.0] │
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
      elsif Utils.strlike?(column)
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
      elsif Utils.strlike?(column)
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
      elsif Utils.strlike?(column)
        col(column.to_s).sum
      elsif column.is_a?(::Array)
        exprs = Utils.selection_to_rbexpr_list(column)
        # TODO
        Utils.wrap_expr(_sum_exprs(exprs))
      else
        fold(lit(0).cast(:u32), ->(a, b) { a + b }, column).alias("sum")
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

    # Count unique values.
    #
    # @return [Object]
    def n_unique(column)
      if column.is_a?(Series)
        column.n_unique
      else
        col(column).n_unique
      end
    end

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

    # Get the last value.
    #
    # Depending on the input type this function does different things:
    #
    # - nil -> expression to take last column of a context.
    # - String -> syntactic sugar for `Polars.col(..).last`
    # - Series -> Take last value in `Series`
    #
    # @return [Object]
    def last(column = nil)
      if column.nil?
        return Utils.wrap_expr(_last)
      end

      if column.is_a?(Series)
        if column.len > 0
          return column[-1]
        else
          raise IndexError, "The series is empty, so no last value can be returned"
        end
      end
      col(column).last
    end

    # Get the first `n` rows.
    #
    # @param column [Object]
    #   Column name or Series.
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Object]
    def head(column, n = 10)
      if column.is_a?(Series)
        column.head(n)
      else
        col(column).head(n)
      end
    end

    # Get the last `n` rows.
    #
    # @param column [Object]
    #   Column name or Series.
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Object]
    def tail(column, n = 10)
      if column.is_a?(Series)
        column.tail(n)
      else
        col(column).tail(n)
      end
    end

    # Return an expression representing a literal value.
    #
    # @return [Expr]
    def lit(value, dtype: nil, allow_object: nil)
      if value.is_a?(::Time) || value.is_a?(::DateTime)
        time_unit = dtype&.time_unit || "ns"
        time_zone = dtype.&time_zone
        e = lit(Utils._datetime_to_pl_timestamp(value, time_unit)).cast(Datetime.new(time_unit))
        if time_zone
          return e.dt.replace_time_zone(time_zone.to_s)
        else
          return e
        end
      elsif value.is_a?(::Date)
        return lit(::Time.utc(value.year, value.month, value.day)).cast(Date)
      elsif value.is_a?(Polars::Series)
        name = value.name
        value = value._s
        e = Utils.wrap_expr(RbExpr.lit(value, allow_object))
        if name == ""
          return e
        end
        return e.alias(name)
      elsif dtype
        return Utils.wrap_expr(RbExpr.lit(value, allow_object)).cast(dtype)
      end

      Utils.wrap_expr(RbExpr.lit(value, allow_object))
    end

    # Cumulatively sum values in a column/Series, or horizontally across list of columns/expressions.
    #
    # @param column [Object]
    #   Column(s) to be used in aggregation.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2],
    #       "b" => [3, 4],
    #       "c" => [5, 6]
    #     }
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 3   ┆ 5   │
    #   # │ 2   ┆ 4   ┆ 6   │
    #   # └─────┴─────┴─────┘
    #
    # @example Cumulatively sum a column by name:
    #   df.select(Polars.cumsum("a"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example Cumulatively sum a list of columns/expressions horizontally:
    #   df.with_column(Polars.cumsum(["a", "c"]))
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬───────────┐
    #   # │ a   ┆ b   ┆ c   ┆ cumsum    │
    #   # │ --- ┆ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64 ┆ i64 ┆ struct[2] │
    #   # ╞═════╪═════╪═════╪═══════════╡
    #   # │ 1   ┆ 3   ┆ 5   ┆ {1,6}     │
    #   # │ 2   ┆ 4   ┆ 6   ┆ {2,8}     │
    #   # └─────┴─────┴─────┴───────────┘
    def cumsum(column)
      if column.is_a?(Series)
        column.cumsum
      elsif Utils.strlike?(column)
        col(column).cumsum
      else
        cumfold(lit(0).cast(:u32), ->(a, b) { a + b }, column).alias("cumsum")
      end
    end

    # Compute the spearman rank correlation between two columns.
    #
    # Missing data will be excluded from the computation.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   Delta degrees of freedom
    # @param propagate_nans [Boolean]
    #   If `True` any `NaN` encountered will lead to `NaN` in the output.
    #   Defaults to `False` where `NaN` are regarded as larger than any finite number
    #   and thus lead to the highest rank.
    #
    # @return [Expr]
    def spearman_rank_corr(a, b, ddof: 1, propagate_nans: false)
      if Utils.strlike?(a)
        a = col(a)
      end
      if Utils.strlike?(b)
        b = col(b)
      end
      Utils.wrap_expr(RbExpr.spearman_rank_corr(a._rbexpr, b._rbexpr, ddof, propagate_nans))
    end

    # Compute the pearson's correlation between two columns.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    # @param ddof [Integer]
    #   Delta degrees of freedom
    #
    # @return [Expr]
    def pearson_corr(a, b, ddof: 1)
      if Utils.strlike?(a)
        a = col(a)
      end
      if Utils.strlike?(b)
        b = col(b)
      end
      Utils.wrap_expr(RbExpr.pearson_corr(a._rbexpr, b._rbexpr, ddof))
    end

    # Compute the covariance between two columns/ expressions.
    #
    # @param a [Object]
    #   Column name or Expression.
    # @param b [Object]
    #   Column name or Expression.
    #
    # @return [Expr]
    def cov(a, b)
      if Utils.strlike?(a)
        a = col(a)
      end
      if Utils.strlike?(b)
        b = col(b)
      end
      Utils.wrap_expr(RbExpr.cov(a._rbexpr, b._rbexpr))
    end

    # def map
    # end

    # def apply
    # end

    # Accumulate over multiple columns horizontally/row wise with a left fold.
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

    # Cumulatively accumulate over multiple columns horizontally/row wise with a left fold.
    #
    # Every cumulative result is added as a separate field in a Struct column.
    #
    # @param acc [Object]
    #   Accumulator Expression. This is the value that will be initialized when the fold
    #   starts. For a sum this could for instance be lit(0).
    # @param f [Object]
    #   Function to apply over the accumulator and the value.
    #   Fn(acc, value) -> new_value
    # @param exprs [Object]
    #   Expressions to aggregate over. May also be a wildcard expression.
    # @param include_init [Boolean]
    #   Include the initial accumulator state as struct field.
    #
    # @return [Object]
    #
    # @note
    #   If you simply want the first encountered expression as accumulator,
    #   consider using `cumreduce`.
    def cumfold(acc, f, exprs, include_init: false)
      acc = Utils.expr_to_lit_or_expr(acc, str_to_lit: true)
      if exprs.is_a?(Expr)
        exprs = [exprs]
      end

      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.cumfold(acc._rbexpr, f, exprs, include_init))
    end

    # def cumreduce
    # end

    # Evaluate columnwise or elementwise with a bitwise OR operation.
    #
    # @return [Expr]
    def any(name)
      if Utils.strlike?(name)
        col(name).any
      else
        fold(lit(false), ->(a, b) { a.cast(:bool) | b.cast(:bool) }, name).alias("any")
      end
    end

    # Exclude certain columns from a wildcard/regex selection.
    #
    # @param columns [Object]
    #   Column(s) to exclude from selection
    #   This can be:
    #
    #   - a column name, or multiple column names
    #   - a regular expression starting with `^` and ending with `$`
    #   - a dtype or multiple dtypes
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "aa" => [1, 2, 3],
    #       "ba" => ["a", "b", nil],
    #       "cc" => [nil, 2.5, 1.5]
    #     }
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬──────┬──────┐
    #   # │ aa  ┆ ba   ┆ cc   │
    #   # │ --- ┆ ---  ┆ ---  │
    #   # │ i64 ┆ str  ┆ f64  │
    #   # ╞═════╪══════╪══════╡
    #   # │ 1   ┆ a    ┆ null │
    #   # │ 2   ┆ b    ┆ 2.5  │
    #   # │ 3   ┆ null ┆ 1.5  │
    #   # └─────┴──────┴──────┘
    #
    # @example Exclude by column name(s):
    #   df.select(Polars.exclude("ba"))
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
    # @example Exclude by regex, e.g. removing all columns whose names end with the letter "a":
    #   df.select(Polars.exclude("^.*a$"))
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
    def exclude(columns)
      col("*").exclude(columns)
    end

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
      elsif Utils.strlike?(name)
        col(name).all
      else
        raise Todo
      end
    end

    # Syntactic sugar for `Polars.col("foo").agg_groups`.
    #
    # @return [Object]
    def groups(column)
      col(column).agg_groups
    end

    # Syntactic sugar for `Polars.col("foo").quantile(...)`.
    #
    # @param column [String]
    #   Column name.
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [Expr]
    def quantile(column, quantile, interpolation: "nearest")
      col(column).quantile(quantile, interpolation: interpolation)
    end

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
    #   Apply an explicit integer dtype to the resulting expression (default is `:i64`).
    #
    # @return [Expr, Series]
    #
    # @example
    #   Polars.arange(0, 3, eager: true)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'arange' [i64]
    #   # [
    #   #         0
    #   #         1
    #   #         2
    #   # ]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2], "b" => [3, 4]})
    #   df.select(Polars.arange(Polars.col("a"), Polars.col("b")))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ arange    │
    #   # │ ---       │
    #   # │ list[i64] │
    #   # ╞═══════════╡
    #   # │ [1, 2]    │
    #   # │ [2, 3]    │
    #   # └───────────┘
    def arange(low, high, step: 1, eager: false, dtype: nil)
      low = Utils.expr_to_lit_or_expr(low, str_to_lit: false)
      high = Utils.expr_to_lit_or_expr(high, str_to_lit: false)
      range_expr = Utils.wrap_expr(RbExpr.arange(low._rbexpr, high._rbexpr, step))

      if !dtype.nil? && !["i64", Int64].include?(dtype)
        range_expr = range_expr.cast(dtype)
      end

      if !eager
        range_expr
      else
        DataFrame.new.select(range_expr.alias("arange")).to_series
      end
    end

    # Find the indexes that would sort the columns.
    #
    # Argsort by multiple columns. The first column will be used for the ordering.
    # If there are duplicates in the first column, the second column will be used to
    # determine the ordering and so on.
    #
    # @param exprs [Object]
    #   Columns use to determine the ordering.
    # @param reverse [Boolean]
    #   Default is ascending.
    #
    # @return [Expr]
    def arg_sort_by(exprs, reverse: false)
      if !exprs.is_a?(::Array)
        exprs = [exprs]
      end
      if reverse == true || reverse == false
        reverse = [reverse] * exprs.length
      end
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.arg_sort_by(exprs, reverse))
    end
    alias_method :argsort_by, :arg_sort_by

    # Create polars `Duration` from distinct time components.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2)],
    #       "add" => [1, 2]
    #     }
    #   )
    #   df.select(
    #     [
    #       (Polars.col("datetime") + Polars.duration(weeks: "add")).alias("add_weeks"),
    #       (Polars.col("datetime") + Polars.duration(days: "add")).alias("add_days"),
    #       (Polars.col("datetime") + Polars.duration(seconds: "add")).alias("add_seconds"),
    #       (Polars.col("datetime") + Polars.duration(milliseconds: "add")).alias(
    #         "add_milliseconds"
    #       ),
    #       (Polars.col("datetime") + Polars.duration(hours: "add")).alias("add_hours")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 5)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬─────────────────────────┬─────────────────────┐
    #   # │ add_weeks           ┆ add_days            ┆ add_seconds         ┆ add_milliseconds        ┆ add_hours           │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---                     ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]            ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪═════════════════════════╪═════════════════════╡
    #   # │ 2022-01-08 00:00:00 ┆ 2022-01-02 00:00:00 ┆ 2022-01-01 00:00:01 ┆ 2022-01-01 00:00:00.001 ┆ 2022-01-01 01:00:00 │
    #   # │ 2022-01-16 00:00:00 ┆ 2022-01-04 00:00:00 ┆ 2022-01-02 00:00:02 ┆ 2022-01-02 00:00:00.002 ┆ 2022-01-02 02:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴─────────────────────────┴─────────────────────┘
    def duration(
      days: nil,
      seconds: nil,
      nanoseconds: nil,
      microseconds: nil,
      milliseconds: nil,
      minutes: nil,
      hours: nil,
      weeks: nil
    )
      if !hours.nil?
        hours = Utils.expr_to_lit_or_expr(hours, str_to_lit: false)._rbexpr
      end
      if !minutes.nil?
        minutes = Utils.expr_to_lit_or_expr(minutes, str_to_lit: false)._rbexpr
      end
      if !seconds.nil?
        seconds = Utils.expr_to_lit_or_expr(seconds, str_to_lit: false)._rbexpr
      end
      if !milliseconds.nil?
        milliseconds = Utils.expr_to_lit_or_expr(milliseconds, str_to_lit: false)._rbexpr
      end
      if !microseconds.nil?
        microseconds = Utils.expr_to_lit_or_expr(microseconds, str_to_lit: false)._rbexpr
      end
      if !nanoseconds.nil?
        nanoseconds = Utils.expr_to_lit_or_expr(nanoseconds, str_to_lit: false)._rbexpr
      end
      if !days.nil?
        days = Utils.expr_to_lit_or_expr(days, str_to_lit: false)._rbexpr
      end
      if !weeks.nil?
        weeks = Utils.expr_to_lit_or_expr(weeks, str_to_lit: false)._rbexpr
      end

      Utils.wrap_expr(
        _rb_duration(
          days,
          seconds,
          nanoseconds,
          microseconds,
          milliseconds,
          minutes,
          hours,
          weeks
        )
      )
    end

    # Horizontally concat Utf8 Series in linear time. Non-Utf8 columns are cast to Utf8.
    #
    # @param exprs [Object]
    #   Columns to concat into a Utf8 Series.
    # @param sep [String]
    #   String value that will be used to separate the values.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["dogs", "cats", nil],
    #       "c" => ["play", "swim", "walk"]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       Polars.concat_str(
    #         [
    #           Polars.col("a") * 2,
    #           Polars.col("b"),
    #           Polars.col("c")
    #         ],
    #         sep: " "
    #       ).alias("full_sentence")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬──────┬───────────────┐
    #   # │ a   ┆ b    ┆ c    ┆ full_sentence │
    #   # │ --- ┆ ---  ┆ ---  ┆ ---           │
    #   # │ i64 ┆ str  ┆ str  ┆ str           │
    #   # ╞═════╪══════╪══════╪═══════════════╡
    #   # │ 1   ┆ dogs ┆ play ┆ 2 dogs play   │
    #   # │ 2   ┆ cats ┆ swim ┆ 4 cats swim   │
    #   # │ 3   ┆ null ┆ walk ┆ null          │
    #   # └─────┴──────┴──────┴───────────────┘
    def concat_str(exprs, sep: "")
      exprs = Utils.selection_to_rbexpr_list(exprs)
      return Utils.wrap_expr(RbExpr.concat_str(exprs, sep))
    end

    # Format expressions as a string.
    #
    # @param fstring [String]
    #   A string that with placeholders.
    #   For example: "hello_{}" or "{}_world
    # @param args [Object]
    #   Expression(s) that fill the placeholders
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a": ["a", "b", "c"],
    #       "b": [1, 2, 3]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.format("foo_{}_bar_{}", Polars.col("a"), "b").alias("fmt")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────┐
    #   # │ fmt         │
    #   # │ ---         │
    #   # │ str         │
    #   # ╞═════════════╡
    #   # │ foo_a_bar_1 │
    #   # │ foo_b_bar_2 │
    #   # │ foo_c_bar_3 │
    #   # └─────────────┘
    def format(fstring, *args)
      if fstring.scan("{}").length != args.length
        raise ArgumentError, "number of placeholders should equal the number of arguments"
      end

      exprs = []

      arguments = args.each
      fstring.split(/(\{\})/).each do |s|
        if s == "{}"
          e = Utils.expr_to_lit_or_expr(arguments.next, str_to_lit: false)
          exprs << e
        elsif s.length > 0
          exprs << lit(s)
        end
      end

      concat_str(exprs, sep: "")
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @return [Expr]
    def concat_list(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(RbExpr.concat_lst(exprs))
    end

    # Collect multiple LazyFrames at the same time.
    #
    # This runs all the computation graphs in parallel on Polars threadpool.
    #
    # @param lazy_frames [Boolean]
    #   A list of LazyFrames to collect.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param string_cache [Boolean]
    #   This argument is deprecated and will be ignored
    # @param no_optimization [Boolean]
    #   Turn off optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param common_subplan_elimination [Boolean]
    #   Will try to cache branching subplans that occur on self-joins or unions.
    # @param allow_streaming [Boolean]
    #   Run parts of the query in a streaming fashion (this is in an alpha state)
    #
    # @return [Array]
    def collect_all(
      lazy_frames,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      string_cache: false,
      no_optimization: false,
      slice_pushdown: true,
      common_subplan_elimination: true,
      allow_streaming: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
      end

      prepared = []

      lazy_frames.each do |lf|
        ldf = lf._ldf.optimization_toggle(
          type_coercion,
          predicate_pushdown,
          projection_pushdown,
          simplify_expression,
          slice_pushdown,
          common_subplan_elimination,
          allow_streaming
        )
        prepared << ldf
      end

      out = _collect_all(prepared)

      # wrap the rbdataframes into dataframe
      result = out.map { |rbdf| Utils.wrap_df(rbdf) }

      result
    end

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
    #   ).select([Polars.struct(Polars.all).alias("my_struct")])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ my_struct           │
    #   # │ ---                 │
    #   # │ struct[4]           │
    #   # ╞═════════════════════╡
    #   # │ {1,"a",true,[1, 2]} │
    #   # │ {2,"b",null,[3]}    │
    #   # └─────────────────────┘
    #
    # @example Only collect specific columns as a struct:
    #   df = Polars::DataFrame.new(
    #     {"a" => [1, 2, 3, 4], "b" => ["one", "two", "three", "four"], "c" => [9, 8, 7, 6]}
    #   )
    #   df.with_column(Polars.struct(Polars.col(["a", "b"])).alias("a_and_b"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬───────┬─────┬─────────────┐
    #   # │ a   ┆ b     ┆ c   ┆ a_and_b     │
    #   # │ --- ┆ ---   ┆ --- ┆ ---         │
    #   # │ i64 ┆ str   ┆ i64 ┆ struct[2]   │
    #   # ╞═════╪═══════╪═════╪═════════════╡
    #   # │ 1   ┆ one   ┆ 9   ┆ {1,"one"}   │
    #   # │ 2   ┆ two   ┆ 8   ┆ {2,"two"}   │
    #   # │ 3   ┆ three ┆ 7   ┆ {3,"three"} │
    #   # │ 4   ┆ four  ┆ 6   ┆ {4,"four"}  │
    #   # └─────┴───────┴─────┴─────────────┘
    def struct(exprs, eager: false)
      if eager
        Polars.select(struct(exprs, eager: false)).to_series
      end
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(_as_struct(exprs))
    end

    # Repeat a single value n times.
    #
    # @param value [Object]
    #   Value to repeat.
    # @param n [Integer]
    #   Repeat `n` times.
    # @param eager [Boolean]
    #   Run eagerly and collect into a `Series`.
    # @param name [String]
    #   Only used in `eager` mode. As expression, use `alias`.
    #
    # @return [Expr]
    def repeat(value, n, eager: false, name: nil)
      if eager
        if name.nil?
          name = ""
        end
        dtype = py_type_to_dtype(type(value))
        Series._repeat(name, value, n, dtype)
      else
        if n.is_a?(Integer)
          n = lit(n)
        end
        Utils.wrap_expr(RbExpr.repeat(value, n._rbexpr))
      end
    end

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

    # Folds the expressions from left to right, keeping the first non-null value.
    #
    # @param exprs [Object]
    #   Expressions to coalesce.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     [
    #       [nil, 1.0, 1.0],
    #       [nil, 2.0, 2.0],
    #       [nil, nil, 3.0],
    #       [nil, nil, nil]
    #     ],
    #     columns: [["a", :f64], ["b", :f64], ["c", :f64]]
    #   )
    #   df.with_column(Polars.coalesce(["a", "b", "c", 99.9]).alias("d"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬──────┬──────┐
    #   # │ a    ┆ b    ┆ c    ┆ d    │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---  │
    #   # │ f64  ┆ f64  ┆ f64  ┆ f64  │
    #   # ╞══════╪══════╪══════╪══════╡
    #   # │ null ┆ 1.0  ┆ 1.0  ┆ 1.0  │
    #   # │ null ┆ 2.0  ┆ 2.0  ┆ 2.0  │
    #   # │ null ┆ null ┆ 3.0  ┆ 3.0  │
    #   # │ null ┆ null ┆ null ┆ 99.9 │
    #   # └──────┴──────┴──────┴──────┘
    def coalesce(exprs, *more_exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      if more_exprs.any?
        exprs.concat(Utils.selection_to_rbexpr_list(more_exprs))
      end
      Utils.wrap_expr(_coalesce_exprs(exprs))
    end

    # Utility function that parses an epoch timestamp (or Unix time) to Polars Date(time).
    #
    # Depending on the `unit` provided, this function will return a different dtype:
    # - unit: "d" returns pl.Date
    # - unit: "s" returns pl.Datetime["us"] (pl.Datetime's default)
    # - unit: "ms" returns pl.Datetime["ms"]
    # - unit: "us" returns pl.Datetime["us"]
    # - unit: "ns" returns pl.Datetime["ns"]
    #
    # @param column [Object]
    #     Series or expression to parse integers to pl.Datetime.
    # @param unit [String]
    #     The unit of the timesteps since epoch time.
    # @param eager [Boolean]
    #     If eager evaluation is `true`, a Series is returned instead of an Expr.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new({"timestamp" => [1666683077, 1666683099]}).lazy
    #   df.select(Polars.from_epoch(Polars.col("timestamp"), unit: "s")).collect
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ timestamp           │
    #   # │ ---                 │
    #   # │ datetime[μs]        │
    #   # ╞═════════════════════╡
    #   # │ 2022-10-25 07:31:17 │
    #   # │ 2022-10-25 07:31:39 │
    #   # └─────────────────────┘
    def from_epoch(column, unit: "s", eager: false)
      if Utils.strlike?(column)
        column = col(column)
      elsif !column.is_a?(Series) && !column.is_a?(Expr)
        column = Series.new(column)
      end

      if unit == "d"
        expr = column.cast(Date)
      elsif unit == "s"
        expr = (column.cast(Int64) * 1_000_000).cast(Datetime.new("us"))
      elsif Utils::DTYPE_TEMPORAL_UNITS.include?(unit)
        expr = column.cast(Datetime.new(unit))
      else
        raise ArgumentError, "'unit' must be one of {{'ns', 'us', 'ms', 's', 'd'}}, got '#{unit}'."
      end

      if eager
        if !column.is_a?(Series)
          raise ArgumentError, "expected Series or Array if eager: true, got #{column.class.name}"
        else
          column.to_frame.select(expr).to_series
        end
      else
        expr
      end
    end

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
    #   # │ 3   ┆ 4   ┆ 1       │
    #   # │ 4   ┆ 0   ┆ 1       │
    #   # └─────┴─────┴─────────┘
    def when(expr)
      expr = Utils.expr_to_lit_or_expr(expr)
      pw = RbExpr.when(expr._rbexpr)
      When.new(pw)
    end
  end
end

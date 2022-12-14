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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 4   ┆ 6   ┆ {2,8}     │
    #   # └─────┴─────┴─────┴───────────┘
    def cumsum(column)
      if column.is_a?(Series)
        column.cumsum
      elsif column.is_a?(String)
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
      if a.is_a?(String)
        a = col(a)
      end
      if b.is_a?(String)
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
      if a.is_a?(String)
        a = col(a)
      end
      if b.is_a?(String)
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
      if a.is_a?(String)
        a = col(a)
      end
      if b.is_a?(String)
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

    # Cumulatively accumulate over multiple columns horizontally/ row wise with a left fold.
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

    # def any
    # end

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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2   ┆ b    ┆ 2.5  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2   ┆ 2.5  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┤
    #   # │ 2.5  │
    #   # ├╌╌╌╌╌╌┤
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
      elsif name.is_a?(String) || name.is_a?(Symbol)
        col(name).all
      else
        raise Todo
      end
    end

    # def groups
    # end

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
    #   ).select([Polars.struct(Polars.all).alias("my_struct")])
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
    #   df.with_column(Polars.struct(Polars.col(["a", "b"])).alias("a_and_b"))
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

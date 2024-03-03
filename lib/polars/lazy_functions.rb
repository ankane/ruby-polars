module Polars
  module LazyFunctions
    # Aggregate to list.
    #
    # @return [Expr]
    def to_list(name)
      col(name).list
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
      Utils.wrap_expr(Plr.fold(acc._rbexpr, f, exprs))
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
    def cum_fold(acc, f, exprs, include_init: false)
      acc = Utils.expr_to_lit_or_expr(acc, str_to_lit: true)
      if exprs.is_a?(Expr)
        exprs = [exprs]
      end

      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.cum_fold(acc._rbexpr, f, exprs, include_init))
    end
    alias_method :cumfold, :cum_fold

    # def cumreduce
    # end

    # Compute two argument arctan in radians.
    #
    # Returns the angle (in radians) in the plane between the
    # positive x-axis and the ray from the origin to (x,y).
    #
    # @param y [Object]
    #   Column name or Expression.
    # @param x [Object]
    #   Column name or Expression.
    #
    # @return [Expr]
    #
    # @example
    #   twoRootTwo = Math.sqrt(2) / 2
    #   df = Polars::DataFrame.new(
    #     {
    #       "y" => [twoRootTwo, -twoRootTwo, twoRootTwo, -twoRootTwo],
    #       "x" => [twoRootTwo, twoRootTwo, -twoRootTwo, -twoRootTwo]
    #     }
    #   )
    #   df.select(
    #     Polars.arctan2d("y", "x").alias("atan2d"), Polars.arctan2("y", "x").alias("atan2")
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────┬───────────┐
    #   # │ atan2d ┆ atan2     │
    #   # │ ---    ┆ ---       │
    #   # │ f64    ┆ f64       │
    #   # ╞════════╪═══════════╡
    #   # │ 45.0   ┆ 0.785398  │
    #   # │ -45.0  ┆ -0.785398 │
    #   # │ 135.0  ┆ 2.356194  │
    #   # │ -135.0 ┆ -2.356194 │
    #   # └────────┴───────────┘
    def arctan2(y, x)
      if Utils.strlike?(y)
        y = col(y)
      end
      if Utils.strlike?(x)
        x = col(x)
      end
      Utils.wrap_expr(Plr.arctan2(y._rbexpr, x._rbexpr))
    end

    # Compute two argument arctan in degrees.
    #
    # Returns the angle (in degrees) in the plane between the positive x-axis
    # and the ray from the origin to (x,y).
    #
    # @param y [Object]
    #   Column name or Expression.
    # @param x [Object]
    #   Column name or Expression.
    #
    # @return [Expr]
    #
    # @example
    #   twoRootTwo = Math.sqrt(2) / 2
    #   df = Polars::DataFrame.new(
    #     {
    #       "y" => [twoRootTwo, -twoRootTwo, twoRootTwo, -twoRootTwo],
    #       "x" => [twoRootTwo, twoRootTwo, -twoRootTwo, -twoRootTwo]
    #     }
    #   )
    #   df.select(
    #     Polars.arctan2d("y", "x").alias("atan2d"), Polars.arctan2("y", "x").alias("atan2")
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────┬───────────┐
    #   # │ atan2d ┆ atan2     │
    #   # │ ---    ┆ ---       │
    #   # │ f64    ┆ f64       │
    #   # ╞════════╪═══════════╡
    #   # │ 45.0   ┆ 0.785398  │
    #   # │ -45.0  ┆ -0.785398 │
    #   # │ 135.0  ┆ 2.356194  │
    #   # │ -135.0 ┆ -2.356194 │
    #   # └────────┴───────────┘
    def arctan2d(y, x)
      if Utils.strlike?(y)
        y = col(y)
      end
      if Utils.strlike?(x)
        x = col(x)
      end
      Utils.wrap_expr(Plr.arctan2d(y._rbexpr, x._rbexpr))
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
    # @param start [Integer, Expr, Series]
    #   Lower bound of range.
    # @param stop [Integer, Expr, Series]
    #   Upper bound of range.
    # @param step [Integer]
    #   Step size of the range.
    # @param eager [Boolean]
    #   If eager evaluation is `True`, a Series is returned instead of an Expr.
    # @param dtype [Symbol]
    #   Apply an explicit integer dtype to the resulting expression (default is `Int64`).
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
    def int_range(start, stop = nil, step: 1, eager: false, dtype: nil)
      if stop.nil?
        stop = start
        start = 0
      end

      start = Utils.parse_as_expression(start)
      stop = Utils.parse_as_expression(stop)
      dtype ||= Int64
      dtype = dtype.to_s if dtype.is_a?(Symbol)
      result = Utils.wrap_expr(Plr.int_range(start, stop, step, dtype)).alias("arange")

      if eager
        return select(result).to_series
      end

      result
    end
    alias_method :arange, :int_range

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
      Utils.wrap_expr(Plr.arg_sort_by(exprs, reverse))
    end
    alias_method :argsort_by, :arg_sort_by

    # Horizontally concat Utf8 Series in linear time. Non-Utf8 columns are cast to Utf8.
    #
    # @param exprs [Object]
    #   Columns to concat into a Utf8 Series.
    # @param sep [String]
    #   String value that will be used to separate the values.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
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
    def concat_str(exprs, sep: "", ignore_nulls: false)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      Utils.wrap_expr(Plr.concat_str(exprs, sep, ignore_nulls))
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
      Utils.wrap_expr(Plr.concat_list(exprs))
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
          allow_streaming,
          false
        )
        prepared << ldf
      end

      out = Plr.collect_all(prepared)

      # wrap the rbdataframes into dataframe
      result = out.map { |rbdf| Utils.wrap_df(rbdf) }

      result
    end

    # Run polars expressions without a context.
    #
    # This is syntactic sugar for running `df.select` on an empty DataFrame.
    #
    # @param exprs [Array]
    #   Column(s) to select, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names,
    #   other non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to select, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    #
    # @example
    #   foo = Polars::Series.new("foo", [1, 2, 3])
    #   bar = Polars::Series.new("bar", [3, 2, 1])
    #   Polars.select(min: Polars.min_horizontal(foo, bar))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ min │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 1   │
    #   # └─────┘
    def select(*exprs, **named_exprs)
      DataFrame.new([]).select(*exprs, **named_exprs)
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
      Utils.wrap_expr(Plr.as_struct(exprs))
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
          raise ArgumentError, "expected 'Series' in 'arg_where' if 'eager: true', got #{condition.class.name}"
        end
        condition.to_frame.select(arg_where(Polars.col(condition.name))).to_series
      else
        condition = Utils.expr_to_lit_or_expr(condition, str_to_lit: true)
        Utils.wrap_expr(Plr.arg_where(condition._rbexpr))
      end
    end

    # Folds the columns from left to right, keeping the first non-null value.
    #
    # @param exprs [Array]
    #   Columns to coalesce. Accepts expression input. Strings are parsed as column
    #   names, other non-expression inputs are parsed as literals.
    # @param more_exprs [Hash]
    #   Additional columns to coalesce, specified as positional arguments.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, nil, nil, nil],
    #       "b" => [1, 2, nil, nil],
    #       "c" => [5, nil, 3, nil]
    #     }
    #   )
    #   df.with_columns(Polars.coalesce(["a", "b", "c", 10]).alias("d"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬──────┬─────┐
    #   # │ a    ┆ b    ┆ c    ┆ d   │
    #   # │ ---  ┆ ---  ┆ ---  ┆ --- │
    #   # │ i64  ┆ i64  ┆ i64  ┆ i64 │
    #   # ╞══════╪══════╪══════╪═════╡
    #   # │ 1    ┆ 1    ┆ 5    ┆ 1   │
    #   # │ null ┆ 2    ┆ null ┆ 2   │
    #   # │ null ┆ null ┆ 3    ┆ 3   │
    #   # │ null ┆ null ┆ null ┆ 10  │
    #   # └──────┴──────┴──────┴─────┘
    #
    # @example
    #   df.with_columns(Polars.coalesce(Polars.col(["a", "b", "c"]), 10.0).alias("d"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬──────┬──────┐
    #   # │ a    ┆ b    ┆ c    ┆ d    │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ i64  ┆ f64  │
    #   # ╞══════╪══════╪══════╪══════╡
    #   # │ 1    ┆ 1    ┆ 5    ┆ 1.0  │
    #   # │ null ┆ 2    ┆ null ┆ 2.0  │
    #   # │ null ┆ null ┆ 3    ┆ 3.0  │
    #   # │ null ┆ null ┆ null ┆ 10.0 │
    #   # └──────┴──────┴──────┴──────┘
    def coalesce(exprs, *more_exprs)
      exprs = Utils.parse_as_list_of_expressions(exprs, *more_exprs)
      Utils.wrap_expr(Plr.coalesce(exprs))
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

    # Parse one or more SQL expressions to polars expression(s).
    #
    # @param sql [Object]
    #   One or more SQL expressions.
    #
    # @return [Expr]
    #
    # @example Parse a single SQL expression:
    #   df = Polars::DataFrame.new({"a" => [2, 1]})
    #   expr = Polars.sql_expr("MAX(a)")
    #   df.select(expr)
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # └─────┘
    #
    # @example Parse multiple SQL expressions:
    #   df.with_columns(
    #     *Polars.sql_expr(["POWER(a,a) AS a_a", "CAST(a AS TEXT) AS a_txt"])
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ a   ┆ a_a ┆ a_txt │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str   │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 2   ┆ 4.0 ┆ 2     │
    #   # │ 1   ┆ 1.0 ┆ 1     │
    #   # └─────┴─────┴───────┘
    def sql_expr(sql)
      if sql.is_a?(::String)
        Utils.wrap_expr(Plr.sql_expr(sql))
      else
        sql.map { |q| Utils.wrap_expr(Plr.sql_expr(q)) }
      end
    end
  end
end

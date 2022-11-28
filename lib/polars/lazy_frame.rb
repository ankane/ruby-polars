module Polars
  # Representation of a Lazy computation graph/query againat a DataFrame.
  class LazyFrame
    # @private
    attr_accessor :_ldf

    # @private
    def self._from_rbldf(rb_ldf)
      ldf = LazyFrame.allocate
      ldf._ldf = rb_ldf
      ldf
    end

    # @private
    def self._scan_csv(
      file,
      has_header: true,
      sep: ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      cache: true,
      with_column_names: nil,
      infer_schema_length: 100,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      parse_dates: false,
      eol_char: "\n"
    )
      dtype_list = nil
      if !dtypes.nil?
        dtype_list = []
        dtypes.each do |k, v|
          dtype_list << [k, Utils.rb_type_to_dtype(v)]
        end
      end
      processed_null_values = Utils._process_null_values(null_values)

      _from_rbldf(
        RbLazyFrame.new_from_csv(
          file,
          sep,
          has_header,
          ignore_errors,
          skip_rows,
          n_rows,
          cache,
          dtype_list,
          low_memory,
          comment_char,
          quote_char,
          processed_null_values,
          infer_schema_length,
          with_column_names,
          rechunk,
          skip_rows_after_header,
          encoding,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          parse_dates,
          eol_char
        )
      )
    end

    # @private
    def self._scan_parquet(
      file,
      n_rows: nil,
      cache: true,
      parallel: "auto",
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      low_memory: false
    )
      _from_rbldf(
        RbLazyFrame.new_from_parquet(
          file,
          n_rows,
          cache,
          parallel,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          low_memory
        )
      )
    end

    # @private
    def self._scan_ipc(
      file,
      n_rows: nil,
      cache: true,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      memory_map: true
    )
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbldf(
        RbLazyFrame.new_from_ipc(
          file,
          n_rows,
          cache,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          memory_map
        )
      )
    end

    # @private
    def self._scan_ndjson(
      file,
      infer_schema_length: nil,
      batch_size: nil,
      n_rows: nil,
      low_memory: false,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0
    )
      _from_rbldf(
        RbLazyFrame.new_from_ndjson(
          file,
          infer_schema_length,
          batch_size,
          n_rows,
          low_memory,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset)
        )
      )
    end

    # def self.from_json
    # end

    # def self.read_json
    # end

    # Get or set column names.
    #
    # @return [Array]
    #
    # @example
    #   df = (
    #      Polars::DataFrame.new(
    #        {
    #          "foo" => [1, 2, 3],
    #          "bar" => [6, 7, 8],
    #          "ham" => ["a", "b", "c"]
    #        }
    #      )
    #      .lazy
    #      .select(["foo", "bar"])
    #   )
    #   df.columns
    #   # => ["foo", "bar"]
    def columns
      _ldf.columns
    end

    # Get dtypes of columns in LazyFrame.
    #
    # @return [Array]
    #
    # @example
    #   lf = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   lf.dtypes
    #   # => [:i64, :f64, :str]
    def dtypes
      _ldf.dtypes
    end

    # Get the schema.
    #
    # @return [Hash]
    #
    # @example
    #   lf = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   lf.schema
    #   # => {"foo"=>:i64, "bar"=>:f64, "ham"=>:str}
    def schema
      _ldf.schema
    end

    # Get the width of the LazyFrame.
    #
    # @return [Integer]
    #
    # @example
    #   lf = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]}).lazy
    #   lf.width
    #   # => 2
    def width
      _ldf.width
    end

    # Check if LazyFrame includes key.
    #
    # @return [Boolean]
    def include?(key)
      columns.include?(key)
    end

    # clone handled by initialize_copy

    # def [](item)
    # end

    # Returns a string representing the LazyFrame.
    #
    # @return [String]
    def to_s
      <<~EOS
        naive plan: (run LazyFrame#describe_optimized_plan to see the optimized plan)

        #{describe_plan}
      EOS
    end

    # def write_json
    # end

    # def pipe
    # end

    # Create a string representation of the unoptimized query plan.
    #
    # @return [String]
    def describe_plan
      _ldf.describe_plan
    end

    # Create a string representation of the optimized query plan.
    #
    # @return [String]
    # def describe_optimized_plan
    # end

    # def show_graph
    # end

    # Sort the DataFrame.
    #
    # Sorting can be done by:
    #
    # - A single column name
    # - An expression
    # - Multiple expressions
    #
    # @param by [Object]
    #   Column (expressions) to sort by.
    # @param reverse [Boolean]
    #   Sort in descending order.
    # @param nulls_last [Boolean]
    #   Place null values last. Can only be used if sorted by a single column.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   df.sort("foo", reverse: true).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    def sort(by, reverse: false, nulls_last: false)
      if by.is_a?(String)
        _from_rbldf(_ldf.sort(by, reverse, nulls_last))
      end
      if Utils.bool?(reverse)
        reverse = [reverse]
      end

      by = Utils.selection_to_rbexpr_list(by)
      _from_rbldf(_ldf.sort_by_exprs(by, reverse, nulls_last))
    end

    # def profile
    # end

    # Collect into a DataFrame.
    #
    # Note: use {#fetch} if you want to run your query on the first `n` rows
    # only. This can be a huge time saver in debugging queries.
    #
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param string_cache [Boolean]
    #   This argument is deprecated. Please set the string cache globally.
    #   The argument will be ignored
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param common_subplan_elimination [Boolean]
    #   Will try to cache branching subplans that occur on self-joins or unions.
    # @param allow_streaming [Boolean]
    #   Run parts of the query in a streaming fashion (this is in an alpha state)
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   df.groupby("a", maintain_order: true).agg(Polars.all.sum).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ a   ┆ 4   ┆ 10  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b   ┆ 11  ┆ 10  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c   ┆ 6   ┆ 1   │
    #   # └─────┴─────┴─────┘
    def collect(
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

      if allow_streaming
        common_subplan_elimination = false
      end

      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming
      )
      Utils.wrap_df(ldf.collect)
    end

    # Collect a small number of rows for debugging purposes.
    #
    # Fetch is like a {#collect} operation, but it overwrites the number of rows
    # read by every scan operation. This is a utility that helps debug a query on a
    # smaller number of rows.
    #
    # Note that the fetch does not guarantee the final number of rows in the
    # DataFrame. Filter, join operations and a lower number of rows available in the
    # scanned file influence the final number of rows.
    #
    # @param n_rows [Integer]
    #   Collect n_rows from the data sources.
    # @param type_coercion [Boolean]
    #   Run type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Run predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Run projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param string_cache [Boolean]
    #   This argument is deprecated. Please set the string cache globally.
    #   The argument will be ignored
    # @param no_optimization [Boolean]
    #   Turn off optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization
    # @param common_subplan_elimination [Boolean]
    #   Will try to cache branching subplans that occur on self-joins or unions.
    # @param allow_streaming [Boolean]
    #   Run parts of the query in a streaming fashion (this is in an alpha state)
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   df.groupby("a", maintain_order: true).agg(Polars.all.sum).fetch(2)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ a   ┆ 1   ┆ 6   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b   ┆ 2   ┆ 5   │
    #   # └─────┴─────┴─────┘
    def fetch(
      n_rows = 500,
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

      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        allow_streaming
      )
      Utils.wrap_df(ldf.fetch(n_rows))
    end

    # Return lazy representation, i.e. itself.
    #
    # Useful for writing code that expects either a `DataFrame` or
    # `LazyFrame`.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil]
    #     }
    #   )
    #   df.lazy
    def lazy
      self
    end

    # Cache the result once the execution of the physical plan hits this node.
    #
    # @return [LazyFrame]
    def cache
      _from_rbldf(_ldf.cache)
    end

    # Create an empty copy of the current LazyFrame.
    #
    # The copy has an identical schema but no data.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil],
    #     }
    #   ).lazy
    #   df.cleared.fetch
    #   # =>
    #   # shape: (0, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ bool │
    #   # ╞═════╪═════╪══════╡
    #   # └─────┴─────┴──────┘
    def cleared
      DataFrame.new(columns: schema).lazy
    end

    # Filter the rows in the DataFrame based on a predicate expression.
    #
    # @param predicate [Object]
    #   Expression that evaluates to a boolean Series.
    #
    # @return [LazyFrame]
    #
    # @example Filter on one condition:
    #   lf = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   lf.filter(Polars.col("foo") < 3).collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example Filter on multiple conditions:
    #   lf.filter((Polars.col("foo") < 3) & (Polars.col("ham") == "a")).collect
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # └─────┴─────┴─────┘
    def filter(predicate)
      _from_rbldf(
        _ldf.filter(
          Utils.expr_to_lit_or_expr(predicate, str_to_lit: false)._rbexpr
        )
      )
    end

    # Select columns from this DataFrame.
    #
    # @param exprs [Object]
    #   Column or columns to select.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"],
    #     }
    #   ).lazy
    #   df.select("foo").collect
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
    #
    # @example
    #   df.select(["foo", "bar"]).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 8   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.col("foo") + 1).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # ├╌╌╌╌╌┤
    #   # │ 4   │
    #   # └─────┘
    #
    # @example
    #   df.select([Polars.col("foo") + 1, Polars.col("bar") + 1]).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 7   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 8   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 4   ┆ 9   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.when(Polars.col("foo") > 2).then(10).otherwise(0)).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────┐
    #   # │ literal │
    #   # │ ---     │
    #   # │ i64     │
    #   # ╞═════════╡
    #   # │ 0       │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 0       │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 10      │
    #   # └─────────┘
    def select(exprs)
      exprs = Utils.selection_to_rbexpr_list(exprs)
      _from_rbldf(_ldf.select(exprs))
    end

    # Start a groupby operation.
    #
    # @param by [Object]
    #   Column(s) to group by.
    # @param maintain_order [Boolean]
    #   Make sure that the order of the groups remain consistent. This is more
    #   expensive than a default groupby.
    #
    # @return [LazyGroupBy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   ).lazy
    #   df.groupby("a", maintain_order: true).agg(Polars.col("b").sum).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b   ┆ 11  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c   ┆ 6   │
    #   # └─────┴─────┘
    def groupby(by, maintain_order: false)
      rbexprs_by = Utils.selection_to_rbexpr_list(by)
      lgb = _ldf.groupby(rbexprs_by, maintain_order)
      LazyGroupBy.new(lgb, self.class)
    end

    # def groupby_rolling
    # end

    # def groupby_dynamic
    # end

    # def join_asof
    # end

    # Add a join operation to the Logical Plan.
    #
    # @param other [LazyFrame]
    #   Lazy DataFrame to join with.
    # @param left_on [Object]
    #   Join column of the left DataFrame.
    # @param right_on [Object]
    #   Join column of the right DataFrame.
    # @param on Object
    #   Join column of both DataFrames. If set, `left_on` and `right_on` should be
    #   None.
    # @param how ["inner", "left", "outer", "semi", "anti", "cross"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   ).lazy
    #   other_df = Polars::DataFrame.new(
    #     {
    #       "apple" => ["x", "y", "z"],
    #       "ham" => ["a", "b", "d"]
    #     }
    #   ).lazy
    #   df.join(other_df, on: "ham").collect
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str ┆ str   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6.0 ┆ a   ┆ x     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "outer").collect
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬─────┬───────┐
    #   # │ foo  ┆ bar  ┆ ham ┆ apple │
    #   # │ ---  ┆ ---  ┆ --- ┆ ---   │
    #   # │ i64  ┆ f64  ┆ str ┆ str   │
    #   # ╞══════╪══════╪═════╪═══════╡
    #   # │ 1    ┆ 6.0  ┆ a   ┆ x     │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2    ┆ 7.0  ┆ b   ┆ y     │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ null ┆ null ┆ d   ┆ z     │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3    ┆ 8.0  ┆ c   ┆ null  │
    #   # └──────┴──────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "left").collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str ┆ str   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6.0 ┆ a   ┆ x     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 8.0 ┆ c   ┆ null  │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "semi").collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "anti").collect
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # └─────┴─────┴─────┘
    def join(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      how: "inner",
      suffix: "_right",
      allow_parallel: true,
      force_parallel: false
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if how == "cross"
        return _from_rbldf(
          _ldf.join(
            other._ldf, [], [], allow_parallel, force_parallel, how, suffix
          )
        )
      end

      if !on.nil?
        rbexprs = Utils.selection_to_rbexpr_list(on)
        rbexprs_left = rbexprs
        rbexprs_right = rbexprs
      elsif !left_on.nil? && !right_on.nil?
        rbexprs_left = Utils.selection_to_rbexpr_list(left_on)
        rbexprs_right = Utils.selection_to_rbexpr_list(right_on)
      else
        raise ArgumentError, "must specify `on` OR `left_on` and `right_on`"
      end

      _from_rbldf(
        self._ldf.join(
          other._ldf,
          rbexprs_left,
          rbexprs_right,
          allow_parallel,
          force_parallel,
          how,
          suffix,
        )
      )
    end

    # Add or overwrite multiple columns in a DataFrame.
    #
    # @param exprs [Object]
    #   List of Expressions that evaluate to columns.
    #
    # @return [LazyFrame]
    #
    # @example
    #   ldf = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   ).lazy
    #   ldf.with_columns(
    #     [
    #       (Polars.col("a") ** 2).alias("a^2"),
    #       (Polars.col("b") / 2).alias("b/2"),
    #       (Polars.col("c").is_not).alias("not c")
    #     ]
    #   ).collect
    #   # =>
    #   # shape: (4, 6)
    #   # ┌─────┬──────┬───────┬──────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2  ┆ b/2  ┆ not c │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪══════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1.0  ┆ 0.25 ┆ false │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4.0  ┆ 2.0  ┆ false │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9.0  ┆ 5.0  ┆ true  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16.0 ┆ 6.5  ┆ false │
    #   # └─────┴──────┴───────┴──────┴──────┴───────┘
    def with_columns(exprs)
      exprs =
        if exprs.nil?
          []
        elsif exprs.is_a?(Expr)
          [exprs]
        else
          exprs.to_a
        end

      rbexprs = []
      exprs.each do |e|
        case e
        when Expr
          rbexprs << e._rbexpr
        when Series
          rbexprs = Utils.lit(e)._rbexpr
        else
          raise ArgumentError, "Expected an expression, got #{e}"
        end
      end

      _from_rbldf(_ldf.with_columns(rbexprs))
    end

    # def with_context
    # end

    # Add or overwrite column in a DataFrame.
    #
    # @param column [Object]
    #   Expression that evaluates to column or a Series to use.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.with_column((Polars.col("b") ** 2).alias("b_squared")).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬───────────┐
    #   # │ a   ┆ b   ┆ b_squared │
    #   # │ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64 ┆ f64       │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ 2   ┆ 4.0       │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 4   ┆ 16.0      │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5   ┆ 6   ┆ 36.0      │
    #   # └─────┴─────┴───────────┘
    #
    # @example
    #   df.with_column(Polars.col("a") ** 2).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬─────┐
    #   # │ a    ┆ b   │
    #   # │ ---  ┆ --- │
    #   # │ f64  ┆ i64 │
    #   # ╞══════╪═════╡
    #   # │ 1.0  ┆ 2   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 9.0  ┆ 4   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 25.0 ┆ 6   │
    #   # └──────┴─────┘
    def with_column(column)
      with_columns([column])
    end

    # Remove one or multiple columns from a DataFrame.
    #
    # @param columns [Object]
    #   - Name of the column that should be removed.
    #   - List of column names.
    #
    # @return [LazyFrame]
    def drop(columns)
      if columns.is_a?(String)
        columns = [columns]
      end
      _from_rbldf(_ldf.drop_columns(columns))
    end

    # Rename column names.
    #
    # @param mapping [Hash]
    #   Key value pairs that map from old name to new name.
    #
    # @return [LazyFrame]
    def rename(mapping)
      existing = mapping.keys
      _new = mapping.values
      _from_rbldf(_ldf.rename(existing, _new))
    end

    # Reverse the DataFrame.
    #
    # @return [LazyFrame]
    def reverse
      _from_rbldf(_ldf.reverse)
    end

    # Shift the values by a given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.shift(1).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ i64  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ null ┆ null │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 1    ┆ 2    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 3    ┆ 4    │
    #   # └──────┴──────┘
    #
    # @example
    #   df.shift(-1).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ i64  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ 3    ┆ 4    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 5    ┆ 6    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ null ┆ null │
    #   # └──────┴──────┘
    def shift(periods)
      _from_rbldf(_ldf.shift(periods))
    end

    # Shift the values by a given period and fill the resulting null values.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill `nil` values with the result of this expression.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.shift_and_fill(1, 0).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 0   ┆ 0   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1   ┆ 2   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 4   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.shift_and_fill(-1, 0).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 5   ┆ 6   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 0   ┆ 0   │
    #   # └─────┴─────┘
    def shift_and_fill(periods, fill_value)
      if !fill_value.is_a?(Expr)
        fill_value = Polars.lit(fill_value)
      end
      _from_rbldf(_ldf.shift_and_fill(periods, fill_value._rbexpr))
    end

    # Get a slice of this DataFrame.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil`, all rows starting at the offset
    #   will be selected.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "b" => [1, 3, 5],
    #       "c" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.slice(1, 2).collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ y   ┆ 3   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ z   ┆ 5   ┆ 6   │
    #   # └─────┴─────┴─────┘
    def slice(offset, length = nil)
      if length && length < 0
        raise ArgumentError, "Negative slice lengths (#{length}) are invalid for LazyFrame"
      end
      _from_rbldf(_ldf.slice(offset, length))
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @note
    #   Consider using the {#fetch} operation if you only want to test your
    #   query. The {#fetch} operation will load the first `n` rows at the scan
    #   level, whereas the {#head}/{#limit} are applied at the end.
    def limit(n = 5)
      head(5)
    end

    # Get the first `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [LazyFrame]
    #
    # @note
    #   Consider using the {#fetch} operation if you only want to test your
    #   query. The {#fetch} operation will load the first `n` rows at the scan
    #   level, whereas the {#head}/{#limit} are applied at the end.
    def head(n = 5)
      slice(0, n)
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #     Number of rows.
    #
    # @return [LazyFrame]
    def tail(n = 5)
      _from_rbldf(_ldf.tail(n))
    end

    # Get the last row of the DataFrame.
    #
    # @return [LazyFrame]
    def last
      tail(1)
    end

    # Get the first row of the DataFrame.
    #
    # @return [LazyFrame]
    def first
      slice(0, 1)
    end

    # def with_row_count
    # end

    # Take every nth row in the LazyFrame and return as a new LazyFrame.
    #
    # @return [LazyFrame]
    #
    # @example
    #   s = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [5, 6, 7, 8]}).lazy
    #   s.take_every(2).collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 5   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 7   │
    #   # └─────┴─────┘
    def take_every(n)
      select(Utils.col("*").take_every(n))
    end

    # def fill_null
    # end

    # Fill floating point NaN values.
    #
    # @param fill_value [Object]
    #   Value to fill the NaN values with.
    #
    # @return [LazyFrame]
    #
    # @note
    #   Note that floating point NaN (Not a Number) are not missing values!
    #   To replace missing values, use `fill_null` instead.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.5, 2, Float::NAN, 4],
    #       "b" => [0.5, 4, Float::NAN, 13],
    #     }
    #   ).lazy
    #   df.fill_nan(99).collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ f64  ┆ f64  │
    #   # ╞══════╪══════╡
    #   # │ 1.5  ┆ 0.5  │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2.0  ┆ 4.0  │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 99.0 ┆ 99.0 │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 4.0  ┆ 13.0 │
    #   # └──────┴──────┘
    def fill_nan(fill_value)
      if !fill_value.is_a?(Expr)
        fill_value = Utils.lit(fill_value)
      end
      _from_rbldf(_ldf.fill_nan(fill_value._rbexpr))
    end

    # Aggregate the columns in the DataFrame to their standard deviation value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.std.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────────┬─────┐
    #   # │ a        ┆ b   │
    #   # │ ---      ┆ --- │
    #   # │ f64      ┆ f64 │
    #   # ╞══════════╪═════╡
    #   # │ 1.290994 ┆ 0.5 │
    #   # └──────────┴─────┘
    #
    # @example
    #   df.std(ddof: 0).collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────────┬──────────┐
    #   # │ a        ┆ b        │
    #   # │ ---      ┆ ---      │
    #   # │ f64      ┆ f64      │
    #   # ╞══════════╪══════════╡
    #   # │ 1.118034 ┆ 0.433013 │
    #   # └──────────┴──────────┘
    def std(ddof: 1)
      _from_rbldf(_ldf.std(ddof))
    end

    # Aggregate the columns in the DataFrame to their variance value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.var.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────────┬──────┐
    #   # │ a        ┆ b    │
    #   # │ ---      ┆ ---  │
    #   # │ f64      ┆ f64  │
    #   # ╞══════════╪══════╡
    #   # │ 1.666667 ┆ 0.25 │
    #   # └──────────┴──────┘
    #
    # @example
    #   df.var(ddof: 0).collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌──────┬────────┐
    #   # │ a    ┆ b      │
    #   # │ ---  ┆ ---    │
    #   # │ f64  ┆ f64    │
    #   # ╞══════╪════════╡
    #   # │ 1.25 ┆ 0.1875 │
    #   # └──────┴────────┘
    def var(ddof: 1)
      _from_rbldf(_ldf.var(ddof))
    end

    # Aggregate the columns in the DataFrame to their maximum value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.max.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 4   ┆ 2   │
    #   # └─────┴─────┘
    def max
      _from_rbldf(_ldf.max)
    end

    # Aggregate the columns in the DataFrame to their minimum value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.min.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 1   │
    #   # └─────┴─────┘
    def min
      _from_rbldf(_ldf.min)
    end

    # Aggregate the columns in the DataFrame to their sum value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.sum.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 10  ┆ 5   │
    #   # └─────┴─────┘
    def sum
      _from_rbldf(_ldf.sum)
    end

    # Aggregate the columns in the DataFrame to their mean value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.mean.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ f64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 2.5 ┆ 1.25 │
    #   # └─────┴──────┘
    def mean
      _from_rbldf(_ldf.mean)
    end

    # Aggregate the columns in the DataFrame to their median value.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.median.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 2.5 ┆ 1.0 │
    #   # └─────┴─────┘
    def median
      _from_rbldf(_ldf.median)
    end

    # Aggregate the columns in the DataFrame to their quantile value.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [1, 2, 1, 1]}).lazy
    #   df.quantile(0.7).collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 3.0 ┆ 1.0 │
    #   # └─────┴─────┘
    def quantile(quantile, interpolation: "nearest")
      _from_rbldf(_ldf.quantile(quantile, interpolation))
    end

    # Explode lists to long format.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["a", "a", "b", "c"],
    #       "numbers" => [[1], [2, 3], [4, 5], [6, 7, 8]],
    #     }
    #   ).lazy
    #   df.explode("numbers").collect
    #   # =>
    #   # shape: (8, 2)
    #   # ┌─────────┬─────────┐
    #   # │ letters ┆ numbers │
    #   # │ ---     ┆ ---     │
    #   # │ str     ┆ i64     │
    #   # ╞═════════╪═════════╡
    #   # │ a       ┆ 1       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ a       ┆ 2       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ a       ┆ 3       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ b       ┆ 4       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ b       ┆ 5       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ c       ┆ 6       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ c       ┆ 7       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ c       ┆ 8       │
    #   # └─────────┴─────────┘
    def explode(columns)
      columns = Utils.selection_to_rbexpr_list(columns)
      _from_rbldf(_ldf.explode(columns))
    end

    # Drop duplicate rows from this DataFrame.
    #
    # Note that this fails if there is a column of type `List` in the DataFrame or
    # subset.
    #
    # @param maintain_order [Boolean]
    #   Keep the same order as the original DataFrame. This requires more work to
    #   compute.
    # @param subset [Object]
    #   Subset to use to compare rows.
    # @param keep ["first", "last"]
    #   Which of the duplicate rows to keep.
    #
    # @return [LazyFrame]
    def unique(maintain_order: true, subset: nil, keep: "first")
      if !subset.nil? && !subset.is_a?(Array)
        subset = [subset]
      end
      _from_rbldf(_ldf.unique(maintain_order, subset, keep))
    end

    # def drop_nulls
    # end

    # def melt
    # end

    # def map
    # end

    # Interpolate intermediate values. The interpolation method is linear.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 9, 10],
    #       "bar" => [6, 7, 9, nil],
    #       "baz" => [1, nil, nil, 9]
    #     }
    #   ).lazy
    #   df.interpolate.collect
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬──────┬─────┐
    #   # │ foo ┆ bar  ┆ baz │
    #   # │ --- ┆ ---  ┆ --- │
    #   # │ i64 ┆ i64  ┆ i64 │
    #   # ╞═════╪══════╪═════╡
    #   # │ 1   ┆ 6    ┆ 1   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 5   ┆ 7    ┆ 3   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 9   ┆ 9    ┆ 6   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 10  ┆ null ┆ 9   │
    #   # └─────┴──────┴─────┘
    def interpolate
      select(Utils.col("*").interpolate)
    end

    # Decompose a struct into its fields.
    #
    # The fields will be inserted into the `DataFrame` on the location of the
    # `struct` type.
    #
    # @param names [Object]
    #   Names of the struct columns that will be decomposed by its fields
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = (
    #     Polars::DataFrame.new(
    #       {
    #         "before" => ["foo", "bar"],
    #         "t_a" => [1, 2],
    #         "t_b" => ["a", "b"],
    #         "t_c" => [true, nil],
    #         "t_d" => [[1, 2], [3]],
    #         "after" => ["baz", "womp"]
    #       }
    #     )
    #     .lazy
    #     .select(
    #       ["before", Polars.struct(Polars.col("^t_.$")).alias("t_struct"), "after"]
    #     )
    #   )
    #   df.fetch
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────┬─────────────────────┬───────┐
    #   # │ before ┆ t_struct            ┆ after │
    #   # │ ---    ┆ ---                 ┆ ---   │
    #   # │ str    ┆ struct[4]           ┆ str   │
    #   # ╞════════╪═════════════════════╪═══════╡
    #   # │ foo    ┆ {1,"a",true,[1, 2]} ┆ baz   │
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ bar    ┆ {2,"b",null,[3]}    ┆ womp  │
    #   # └────────┴─────────────────────┴───────┘
    #
    # @example
    #   df.unnest("t_struct").fetch
    #   # =>
    #   # shape: (2, 6)
    #   # ┌────────┬─────┬─────┬──────┬───────────┬───────┐
    #   # │ before ┆ t_a ┆ t_b ┆ t_c  ┆ t_d       ┆ after │
    #   # │ ---    ┆ --- ┆ --- ┆ ---  ┆ ---       ┆ ---   │
    #   # │ str    ┆ i64 ┆ str ┆ bool ┆ list[i64] ┆ str   │
    #   # ╞════════╪═════╪═════╪══════╪═══════════╪═══════╡
    #   # │ foo    ┆ 1   ┆ a   ┆ true ┆ [1, 2]    ┆ baz   │
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ bar    ┆ 2   ┆ b   ┆ null ┆ [3]       ┆ womp  │
    #   # └────────┴─────┴─────┴──────┴───────────┴───────┘
    def unnest(names)
      if names.is_a?(String)
        names = [names]
      end
      _from_rbldf(_ldf.unnest(names))
    end

    private

    def initialize_copy(other)
      super
      self._ldf = _ldf._clone
    end

    def _from_rbldf(rb_ldf)
      self.class._from_rbldf(rb_ldf)
    end
  end
end

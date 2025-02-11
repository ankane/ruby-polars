module Polars
  # Representation of a Lazy computation graph/query against a DataFrame.
  class LazyFrame
    # @private
    attr_accessor :_ldf

    # Create a new LazyFrame.
    def initialize(data = nil, schema: nil, schema_overrides: nil, orient: nil, infer_schema_length: 100, nan_to_null: false)
      self._ldf = (
        DataFrame.new(
          data,
          schema: schema,
          schema_overrides: schema_overrides,
          orient: orient,
          infer_schema_length: infer_schema_length,
          nan_to_null: nan_to_null
        )
        .lazy
        ._ldf
      )
    end

    # @private
    def self._from_rbldf(rb_ldf)
      ldf = LazyFrame.allocate
      ldf._ldf = rb_ldf
      ldf
    end

    # def self.from_json
    # end

    # Read a logical plan from a JSON file to construct a LazyFrame.
    #
    # @param file [String]
    #   Path to a file or a file-like object.
    #
    # @return [LazyFrame]
    def self.read_json(file)
      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end

      Utils.wrap_ldf(RbLazyFrame.read_json(file))
    end

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
      _ldf.collect_schema.keys
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
    #   # => [Polars::Int64, Polars::Float64, Polars::String]
    def dtypes
      _ldf.collect_schema.values
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
    #   # => {"foo"=>Polars::Int64, "bar"=>Polars::Float64, "ham"=>Polars::String}
    def schema
      _ldf.collect_schema
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
      _ldf.collect_schema.length
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

    # Write the logical plan of this LazyFrame to a file or string in JSON format.
    #
    # @param file [String]
    #   File path to which the result should be written.
    #
    # @return [nil]
    def write_json(file)
      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end
      _ldf.write_json(file)
      nil
    end

    # Offers a structured way to apply a sequence of user-defined functions (UDFs).
    #
    # @param func [Object]
    #   Callable; will receive the frame as the first parameter,
    #   followed by any given args/kwargs.
    # @param args [Object]
    #   Arguments to pass to the UDF.
    # @param kwargs [Object]
    #   Keyword arguments to pass to the UDF.
    #
    # @return [LazyFrame]
    #
    # @example
    #   cast_str_to_int = lambda do |data, col_name:|
    #     data.with_column(Polars.col(col_name).cast(:i64))
    #   end
    #
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => ["10", "20", "30", "40"]}).lazy
    #   df.pipe(cast_str_to_int, col_name: "b").collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 10  │
    #   # │ 2   ┆ 20  │
    #   # │ 3   ┆ 30  │
    #   # │ 4   ┆ 40  │
    #   # └─────┴─────┘
    def pipe(func, *args, **kwargs, &block)
      func.call(self, *args, **kwargs, &block)
    end

    # Create a string representation of the unoptimized query plan.
    #
    # @return [String]
    def describe_plan
      _ldf.describe_plan
    end

    # Create a string representation of the optimized query plan.
    #
    # @return [String]
    def describe_optimized_plan(
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      slice_pushdown: true,
      common_subplan_elimination: true,
      comm_subexpr_elim: true,
      allow_streaming: false
    )
      ldf = _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        common_subplan_elimination,
        comm_subexpr_elim,
        allow_streaming,
        false
      )

      ldf.describe_optimized_plan
    end

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
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    def sort(by, *more_by, reverse: false, nulls_last: false, maintain_order: false, multithreaded: true)
      if by.is_a?(::String) && more_by.empty?
        return _from_rbldf(
          _ldf.sort(
            by, reverse, nulls_last, maintain_order, multithreaded
          )
        )
      end

      by = Utils.parse_into_list_of_expressions(by, *more_by)
      reverse = Utils.extend_bool(reverse, by.length, "reverse", "by")
      nulls_last = Utils.extend_bool(nulls_last, by.length, "nulls_last", "by")
      _from_rbldf(
        _ldf.sort_by_exprs(
          by, reverse, nulls_last, maintain_order, multithreaded
        )
      )
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
    #   df.group_by("a", maintain_order: true).agg(Polars.all.sum).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ a   ┆ 4   ┆ 10  │
    #   # │ b   ┆ 11  ┆ 10  │
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
      comm_subexpr_elim: true,
      allow_streaming: false,
      _eager: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
        common_subplan_elimination = false
        comm_subexpr_elim = false
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
        comm_subexpr_elim,
        allow_streaming,
        _eager
      )
      Utils.wrap_df(ldf.collect)
    end

    # Persists a LazyFrame at the provided path.
    #
    # This allows streaming results that are larger than RAM to be written to disk.
    #
    # @param path [String]
    #   File path to which the file should be written.
    # @param compression ["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"]
    #   Choose "zstd" for good compression performance.
    #   Choose "lz4" for fast compression/decompression.
    #   Choose "snappy" for more backwards compatibility guarantees
    #   when you deal with older parquet readers.
    # @param compression_level [Integer]
    #   The level of compression to use. Higher compression means smaller files on
    #   disk.
    #
    #   - "gzip" : min-level: 0, max-level: 10.
    #   - "brotli" : min-level: 0, max-level: 11.
    #   - "zstd" : min-level: 1, max-level: 22.
    # @param statistics [Boolean]
    #   Write statistics to the parquet headers. This requires extra compute.
    # @param row_group_size [Integer]
    #   Size of the row groups in number of rows.
    #   If `nil` (default), the chunks of the `DataFrame` are
    #   used. Writing in smaller chunks may reduce memory pressure and improve
    #   writing speeds.
    # @param data_pagesize_limit [Integer]
    #   Size limit of individual data pages.
    #   If not set defaults to 1024 * 1024 bytes
    # @param maintain_order [Boolean]
    #   Maintain the order in which data is processed.
    #   Setting this to `false` will  be slightly faster.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    #
    # @return [DataFrame]
    #
    # @example
    #   lf = Polars.scan_csv("/path/to/my_larger_than_ram_file.csv")
    #   lf.sink_parquet("out.parquet")
    def sink_parquet(
      path,
      compression: "zstd",
      compression_level: nil,
      statistics: true,
      row_group_size: nil,
      data_pagesize_limit: nil,
      maintain_order: true,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      no_optimization: false,
      slice_pushdown: true,
      storage_options: nil,
      retries: 2
    )
      lf = _set_sink_optimizations(
        type_coercion: type_coercion,
        predicate_pushdown: predicate_pushdown,
        projection_pushdown: projection_pushdown,
        simplify_expression: simplify_expression,
        slice_pushdown: slice_pushdown,
        no_optimization: no_optimization
      )

      if statistics == true
        statistics = {
          min: true,
          max: true,
          distinct_count: false,
          null_count: true
        }
      elsif statistics == false
        statistics = {}
      elsif statistics == "full"
        statistics = {
          min: true,
          max: true,
          distinct_count: true,
          null_count: true
        }
      end

      if storage_options&.any?
        storage_options = storage_options.to_a
      else
        storage_options = nil
      end

      lf.sink_parquet(
        path,
        compression,
        compression_level,
        statistics,
        row_group_size,
        data_pagesize_limit,
        maintain_order,
        storage_options,
        retries
      )
    end

    # Evaluate the query in streaming mode and write to an IPC file.
    #
    # This allows streaming results that are larger than RAM to be written to disk.
    #
    # @param path [String]
    #   File path to which the file should be written.
    # @param compression ["lz4", "zstd"]
    #   Choose "zstd" for good compression performance.
    #   Choose "lz4" for fast compression/decompression.
    # @param maintain_order [Boolean]
    #   Maintain the order in which data is processed.
    #   Setting this to `false` will  be slightly faster.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    #
    # @return [DataFrame]
    #
    # @example
    #   lf = Polars.scan_csv("/path/to/my_larger_than_ram_file.csv")
    #   lf.sink_ipc("out.arrow")
    def sink_ipc(
      path,
      compression: "zstd",
      maintain_order: true,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      slice_pushdown: true,
      no_optimization: false
    )
      # TODO support storage options in Rust
      storage_options = nil
      retries = 2

      lf = _set_sink_optimizations(
        type_coercion: type_coercion,
        predicate_pushdown: predicate_pushdown,
        projection_pushdown: projection_pushdown,
        simplify_expression: simplify_expression,
        slice_pushdown: slice_pushdown,
        no_optimization: no_optimization
      )

      if storage_options&.any?
        storage_options = storage_options.to_a
      else
        storage_options = nil
      end

      lf.sink_ipc(
        path,
        compression,
        maintain_order,
        storage_options,
        retries
      )
    end

    # Evaluate the query in streaming mode and write to a CSV file.
    #
    # This allows streaming results that are larger than RAM to be written to disk.
    #
    # @param path [String]
    #   File path to which the file should be written.
    # @param include_bom [Boolean]
    #   Whether to include UTF-8 BOM in the CSV output.
    # @param include_header [Boolean]
    #   Whether to include header in the CSV output.
    # @param separator [String]
    #   Separate CSV fields with this symbol.
    # @param line_terminator [String]
    #   String used to end each row.
    # @param quote_char [String]
    #   Byte to use as quoting character.
    # @param batch_size [Integer]
    #   Number of rows that will be processed per thread.
    # @param datetime_format [String]
    #   A format string, with the specifiers defined by the
    #   `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
    #   Rust crate. If no format specified, the default fractional-second
    #   precision is inferred from the maximum timeunit found in the frame's
    #   Datetime cols (if any).
    # @param date_format [String]
    #   A format string, with the specifiers defined by the
    #   `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
    #   Rust crate.
    # @param time_format [String]
    #   A format string, with the specifiers defined by the
    #   `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
    #   Rust crate.
    # @param float_precision [Integer]
    #   Number of decimal places to write, applied to both `Float32` and
    #   `Float64` datatypes.
    # @param null_value [String]
    #   A string representing null values (defaulting to the empty string).
    # @param quote_style ["necessary", "always", "non_numeric", "never"]
    #   Determines the quoting strategy used.
    #
    #   - necessary (default): This puts quotes around fields only when necessary.
    #     They are necessary when fields contain a quote,
    #     delimiter or record terminator.
    #     Quotes are also necessary when writing an empty record
    #     (which is indistinguishable from a record with one empty field).
    #     This is the default.
    #   - always: This puts quotes around every field. Always.
    #   - never: This never puts quotes around fields, even if that results in
    #     invalid CSV data (e.g.: by not quoting strings containing the
    #     separator).
    #   - non_numeric: This puts quotes around all fields that are non-numeric.
    #     Namely, when writing a field that does not parse as a valid float
    #     or integer, then quotes will be used even if they aren`t strictly
    #     necessary.
    # @param maintain_order [Boolean]
    #   Maintain the order in which data is processed.
    #   Setting this to `false` will  be slightly faster.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    #
    # @return [DataFrame]
    #
    # @example
    #   lf = Polars.scan_csv("/path/to/my_larger_than_ram_file.csv")
    #   lf.sink_csv("out.csv")
    def sink_csv(
      path,
      include_bom: false,
      include_header: true,
      separator: ",",
      line_terminator: "\n",
      quote_char: '"',
      batch_size: 1024,
      datetime_format: nil,
      date_format: nil,
      time_format: nil,
      float_scientific: nil,
      float_precision: nil,
      null_value: nil,
      quote_style: nil,
      maintain_order: true,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      slice_pushdown: true,
      no_optimization: false
    )
      Utils._check_arg_is_1byte("separator", separator, false)
      Utils._check_arg_is_1byte("quote_char", quote_char, false)

      lf = _set_sink_optimizations(
        type_coercion: type_coercion,
        predicate_pushdown: predicate_pushdown,
        projection_pushdown: projection_pushdown,
        simplify_expression: simplify_expression,
        slice_pushdown: slice_pushdown,
        no_optimization: no_optimization
      )

      lf.sink_csv(
        path,
        include_bom,
        include_header,
        separator.ord,
        line_terminator,
        quote_char.ord,
        batch_size,
        datetime_format,
        date_format,
        time_format,
        float_scientific,
        float_precision,
        null_value,
        quote_style,
        maintain_order
      )
    end

    # Evaluate the query in streaming mode and write to an NDJSON file.
    #
    # This allows streaming results that are larger than RAM to be written to disk.
    #
    # @param path [String]
    #   File path to which the file should be written.
    # @param maintain_order [Boolean]
    #   Maintain the order in which data is processed.
    #   Setting this to `false` will be slightly faster.
    # @param type_coercion [Boolean]
    #   Do type coercion optimization.
    # @param predicate_pushdown [Boolean]
    #   Do predicate pushdown optimization.
    # @param projection_pushdown [Boolean]
    #   Do projection pushdown optimization.
    # @param simplify_expression [Boolean]
    #   Run simplify expressions optimization.
    # @param slice_pushdown [Boolean]
    #   Slice pushdown optimization.
    # @param no_optimization [Boolean]
    #   Turn off (certain) optimizations.
    #
    # @return [DataFrame]
    #
    # @example
    #   lf = Polars.scan_csv("/path/to/my_larger_than_ram_file.csv")
    #   lf.sink_ndjson("out.ndjson")
    def sink_ndjson(
      path,
      maintain_order: true,
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      slice_pushdown: true,
      no_optimization: false,
      storage_options: nil,
      retries: 2
    )
      lf = _set_sink_optimizations(
        type_coercion: type_coercion,
        predicate_pushdown: predicate_pushdown,
        projection_pushdown: projection_pushdown,
        simplify_expression: simplify_expression,
        slice_pushdown: slice_pushdown,
        no_optimization: no_optimization
      )

      if storage_options&.any?
        storage_options = storage_options.to_a
      else
        storage_options = nil
      end

      lf.sink_json(path, maintain_order, storage_options, retries)
    end

    # @private
    def _set_sink_optimizations(
      type_coercion: true,
      predicate_pushdown: true,
      projection_pushdown: true,
      simplify_expression: true,
      slice_pushdown: true,
      no_optimization: false
    )
      if no_optimization
        predicate_pushdown = false
        projection_pushdown = false
        slice_pushdown = false
      end

      _ldf.optimization_toggle(
        type_coercion,
        predicate_pushdown,
        projection_pushdown,
        simplify_expression,
        slice_pushdown,
        false,
        false,
        true,
        false
      )
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
    #   df.group_by("a", maintain_order: true).agg(Polars.all.sum).fetch(2)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ a   ┆ 1   ┆ 6   │
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
      comm_subexpr_elim: true,
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
        comm_subexpr_elim,
        allow_streaming,
        false
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

    # Cast LazyFrame column(s) to the specified dtype(s).
    #
    # @param dtypes [Hash]
    #   Mapping of column names (or selector) to dtypes, or a single dtype
    #   to which all columns will be cast.
    # @param strict [Boolean]
    #   Throw an error if a cast could not be done (for instance, due to an
    #   overflow).
    #
    # @return [LazyFrame]
    #
    # @example Cast specific frame columns to the specified dtypes:
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => [Date.new(2020, 1, 2), Date.new(2021, 3, 4), Date.new(2022, 5, 6)]
    #     }
    #   )
    #   lf.cast({"foo" => Polars::Float32, "bar" => Polars::UInt8}).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬────────────┐
    #   # │ foo ┆ bar ┆ ham        │
    #   # │ --- ┆ --- ┆ ---        │
    #   # │ f32 ┆ u8  ┆ date       │
    #   # ╞═════╪═════╪════════════╡
    #   # │ 1.0 ┆ 6   ┆ 2020-01-02 │
    #   # │ 2.0 ┆ 7   ┆ 2021-03-04 │
    #   # │ 3.0 ┆ 8   ┆ 2022-05-06 │
    #   # └─────┴─────┴────────────┘
    #
    # @example Cast all frame columns matching one dtype (or dtype group) to another dtype:
    #   lf.cast({Polars::Date => Polars::Datetime}).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────────────────────┐
    #   # │ foo ┆ bar ┆ ham                 │
    #   # │ --- ┆ --- ┆ ---                 │
    #   # │ i64 ┆ f64 ┆ datetime[μs]        │
    #   # ╞═════╪═════╪═════════════════════╡
    #   # │ 1   ┆ 6.0 ┆ 2020-01-02 00:00:00 │
    #   # │ 2   ┆ 7.0 ┆ 2021-03-04 00:00:00 │
    #   # │ 3   ┆ 8.0 ┆ 2022-05-06 00:00:00 │
    #   # └─────┴─────┴─────────────────────┘
    #
    # @example Cast all frame columns to the specified dtype:
    #   lf.cast(Polars::String).collect.to_h(as_series: false)
    #   # => {"foo"=>["1", "2", "3"], "bar"=>["6.0", "7.0", "8.0"], "ham"=>["2020-01-02", "2021-03-04", "2022-05-06"]}
    def cast(dtypes, strict: true)
      if !dtypes.is_a?(Hash)
        return _from_rbldf(_ldf.cast_all(dtypes, strict))
      end

      cast_map = {}
      dtypes.each do |c, dtype|
        dtype = Utils.parse_into_dtype(dtype)
        cast_map.merge!(
          c.is_a?(::String) ? {c => dtype} : Utils.expand_selector(self, c).to_h { |x| [x, dtype] }
        )
      end

      _from_rbldf(_ldf.cast(cast_map, strict))
    end

    # Create an empty copy of the current LazyFrame.
    #
    # The copy has an identical schema but no data.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil],
    #     }
    #   ).lazy
    #   lf.clear.fetch
    #   # =>
    #   # shape: (0, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ bool │
    #   # ╞═════╪═════╪══════╡
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   lf.clear(2).fetch
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ a    ┆ b    ┆ c    │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ f64  ┆ bool │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ null ┆ null ┆ null │
    #   # └──────┴──────┴──────┘
    def clear(n = 0)
      DataFrame.new(columns: schema).clear(n).lazy
    end
    alias_method :cleared, :clear

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
          Utils.parse_into_expression(predicate, str_as_lit: false)
        )
      )
    end

    # Select columns from this DataFrame.
    #
    # @param exprs [Array]
    #   Column(s) to select, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names,
    #   other non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to select, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
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
    #   # │ 2   │
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
    #   # │ 2   ┆ 7   │
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
    #   # │ 3   │
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
    #   # │ 3   ┆ 8   │
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
    #   # │ i32     │
    #   # ╞═════════╡
    #   # │ 0       │
    #   # │ 0       │
    #   # │ 10      │
    #   # └─────────┘
    def select(*exprs, **named_exprs)
      structify = ENV.fetch("POLARS_AUTO_STRUCTIFY", "0") != "0"

      rbexprs = Utils.parse_into_list_of_expressions(
        *exprs, **named_exprs, __structify: structify
      )
      _from_rbldf(_ldf.select(rbexprs))
    end

    # Start a group by operation.
    #
    # @param by [Array]
    #   Column(s) to group by.
    # @param maintain_order [Boolean]
    #   Make sure that the order of the groups remain consistent. This is more
    #   expensive than a default group by.
    # @param named_by [Hash]
    #   Additional columns to group by, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
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
    #   df.group_by("a", maintain_order: true).agg(Polars.col("b").sum).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 4   │
    #   # │ b   ┆ 11  │
    #   # │ c   ┆ 6   │
    #   # └─────┴─────┘
    def group_by(*by, maintain_order: false, **named_by)
      exprs = Utils.parse_into_list_of_expressions(*by, **named_by)
      lgb = _ldf.group_by(exprs, maintain_order)
      LazyGroupBy.new(lgb)
    end
    alias_method :groupby, :group_by
    alias_method :group, :group_by

    # Create rolling groups based on a time column.
    #
    # Also works for index values of type `:i32` or `:i64`.
    #
    # Different from a `dynamic_group_by` the windows are now determined by the
    # individual values and are not of constant intervals. For constant intervals
    # use *group_by_dynamic*.
    #
    # The `period` and `offset` arguments are created either from a timedelta, or
    # by using the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # In case of a group_by_rolling on an integer column, the windows are defined by:
    #
    # - "1i"      # length 1
    # - "10i"     # length 10
    #
    # @param index_column [Object]
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a rolling group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param period [Object]
    #   Length of the window.
    # @param offset [Object]
    #   Offset of the window. Default is -period.
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param by [Object]
    #   Also group by this column/these columns.
    #
    # @return [LazyFrame]
    #
    # @example
    #   dates = [
    #     "2020-01-01 13:45:48",
    #     "2020-01-01 16:42:13",
    #     "2020-01-01 16:45:09",
    #     "2020-01-02 18:12:48",
    #     "2020-01-03 19:45:32",
    #     "2020-01-08 23:16:43"
    #   ]
    #   df = Polars::LazyFrame.new({"dt" => dates, "a" => [3, 7, 5, 9, 2, 1]}).with_column(
    #     Polars.col("dt").str.strptime(Polars::Datetime).set_sorted
    #   )
    #   df.rolling(index_column: "dt", period: "2d").agg(
    #     [
    #       Polars.sum("a").alias("sum_a"),
    #       Polars.min("a").alias("min_a"),
    #       Polars.max("a").alias("max_a")
    #     ]
    #   ).collect
    #   # =>
    #   # shape: (6, 4)
    #   # ┌─────────────────────┬───────┬───────┬───────┐
    #   # │ dt                  ┆ sum_a ┆ min_a ┆ max_a │
    #   # │ ---                 ┆ ---   ┆ ---   ┆ ---   │
    #   # │ datetime[μs]        ┆ i64   ┆ i64   ┆ i64   │
    #   # ╞═════════════════════╪═══════╪═══════╪═══════╡
    #   # │ 2020-01-01 13:45:48 ┆ 3     ┆ 3     ┆ 3     │
    #   # │ 2020-01-01 16:42:13 ┆ 10    ┆ 3     ┆ 7     │
    #   # │ 2020-01-01 16:45:09 ┆ 15    ┆ 3     ┆ 7     │
    #   # │ 2020-01-02 18:12:48 ┆ 24    ┆ 3     ┆ 9     │
    #   # │ 2020-01-03 19:45:32 ┆ 11    ┆ 2     ┆ 9     │
    #   # │ 2020-01-08 23:16:43 ┆ 1     ┆ 1     ┆ 1     │
    #   # └─────────────────────┴───────┴───────┴───────┘
    def rolling(
      index_column:,
      period:,
      offset: nil,
      closed: "right",
      by: nil
    )
      index_column = Utils.parse_into_expression(index_column)
      if offset.nil?
        offset = Utils.negate_duration_string(Utils.parse_as_duration_string(period))
      end

      rbexprs_by = (
        !by.nil? ? Utils.parse_into_list_of_expressions(by) : []
      )
      period = Utils.parse_as_duration_string(period)
      offset = Utils.parse_as_duration_string(offset)

      lgb = _ldf.rolling(index_column, period, offset, closed, rbexprs_by)
      LazyGroupBy.new(lgb)
    end
    alias_method :group_by_rolling, :rolling
    alias_method :groupby_rolling, :rolling

    # Group based on a time value (or index value of type `:i32`, `:i64`).
    #
    # Time windows are calculated and rows are assigned to windows. Different from a
    # normal group by is that a row can be member of multiple groups. The time/index
    # window could be seen as a rolling window, with a window size determined by
    # dates/times/values instead of slots in the DataFrame.
    #
    # A window is defined by:
    #
    # - every: interval of the window
    # - period: length of the window
    # - offset: offset of the window
    #
    # The `every`, `period` and `offset` arguments are created with
    # the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # In case of a group_by_dynamic on an integer column, the windows are defined by:
    #
    # - "1i"      # length 1
    # - "10i"     # length 10
    #
    # @param index_column [Object]
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a dynamic group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param every [Object]
    #   Interval of the window.
    # @param period [Object]
    #   Length of the window, if None it is equal to 'every'.
    # @param offset [Object]
    #   Offset of the window if None and period is None it will be equal to negative
    #   `every`.
    # @param truncate [Boolean]
    #   Truncate the time value to the window lower bound.
    # @param include_boundaries [Boolean]
    #   Add the lower and upper bound of the window to the "_lower_bound" and
    #   "_upper_bound" columns. This will impact performance because it's harder to
    #   parallelize
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param by [Object]
    #   Also group by this column/these columns
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => Polars.datetime_range(
    #         DateTime.new(2021, 12, 16),
    #         DateTime.new(2021, 12, 16, 3),
    #         "30m",
    #         time_unit: "us",
    #         eager: true
    #       ),
    #       "n" => 0..6
    #     }
    #   )
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────┐
    #   # │ time                ┆ n   │
    #   # │ ---                 ┆ --- │
    #   # │ datetime[μs]        ┆ i64 │
    #   # ╞═════════════════════╪═════╡
    #   # │ 2021-12-16 00:00:00 ┆ 0   │
    #   # │ 2021-12-16 00:30:00 ┆ 1   │
    #   # │ 2021-12-16 01:00:00 ┆ 2   │
    #   # │ 2021-12-16 01:30:00 ┆ 3   │
    #   # │ 2021-12-16 02:00:00 ┆ 4   │
    #   # │ 2021-12-16 02:30:00 ┆ 5   │
    #   # │ 2021-12-16 03:00:00 ┆ 6   │
    #   # └─────────────────────┴─────┘
    #
    # @example Group by windows of 1 hour starting at 2021-12-16 00:00:00.
    #   df.group_by_dynamic("time", every: "1h", closed: "right").agg(
    #     [
    #       Polars.col("time").min.alias("time_min"),
    #       Polars.col("time").max.alias("time_max")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┐
    #   # │ time                ┆ time_min            ┆ time_max            │
    #   # │ ---                 ┆ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-16 00:00:00 │
    #   # │ 2021-12-16 00:00:00 ┆ 2021-12-16 00:30:00 ┆ 2021-12-16 01:00:00 │
    #   # │ 2021-12-16 01:00:00 ┆ 2021-12-16 01:30:00 ┆ 2021-12-16 02:00:00 │
    #   # │ 2021-12-16 02:00:00 ┆ 2021-12-16 02:30:00 ┆ 2021-12-16 03:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┘
    #
    # @example The window boundaries can also be added to the aggregation result.
    #   df.group_by_dynamic(
    #     "time", every: "1h", include_boundaries: true, closed: "right"
    #   ).agg([Polars.col("time").count.alias("time_count")])
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
    #   # │ _lower_boundary     ┆ _upper_boundary     ┆ time                ┆ time_count │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        ┆ u32        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ 2021-12-16 00:00:00 ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 00:00:00 ┆ 2          │
    #   # │ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 2          │
    #   # │ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 2          │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴────────────┘
    #
    # @example When closed="left", should not include right end of interval.
    #   df.group_by_dynamic("time", every: "1h", closed: "left").agg(
    #     [
    #       Polars.col("time").count.alias("time_count"),
    #       Polars.col("time").alias("time_agg_list")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬────────────┬─────────────────────────────────┐
    #   # │ time                ┆ time_count ┆ time_agg_list                   │
    #   # │ ---                 ┆ ---        ┆ ---                             │
    #   # │ datetime[μs]        ┆ u32        ┆ list[datetime[μs]]              │
    #   # ╞═════════════════════╪════════════╪═════════════════════════════════╡
    #   # │ 2021-12-16 00:00:00 ┆ 2          ┆ [2021-12-16 00:00:00, 2021-12-… │
    #   # │ 2021-12-16 01:00:00 ┆ 2          ┆ [2021-12-16 01:00:00, 2021-12-… │
    #   # │ 2021-12-16 02:00:00 ┆ 2          ┆ [2021-12-16 02:00:00, 2021-12-… │
    #   # │ 2021-12-16 03:00:00 ┆ 1          ┆ [2021-12-16 03:00:00]           │
    #   # └─────────────────────┴────────────┴─────────────────────────────────┘
    #
    # @example When closed="both" the time values at the window boundaries belong to 2 groups.
    #   df.group_by_dynamic("time", every: "1h", closed: "both").agg(
    #     [Polars.col("time").count.alias("time_count")]
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────────────────┬────────────┐
    #   # │ time                ┆ time_count │
    #   # │ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ u32        │
    #   # ╞═════════════════════╪════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ 2021-12-16 00:00:00 ┆ 3          │
    #   # │ 2021-12-16 01:00:00 ┆ 3          │
    #   # │ 2021-12-16 02:00:00 ┆ 3          │
    #   # │ 2021-12-16 03:00:00 ┆ 1          │
    #   # └─────────────────────┴────────────┘
    #
    # @example Dynamic group bys can also be combined with grouping on normal keys.
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => Polars.datetime_range(
    #         DateTime.new(2021, 12, 16),
    #         DateTime.new(2021, 12, 16, 3),
    #         "30m",
    #         time_unit: "us",
    #         eager: true
    #       ),
    #       "groups" => ["a", "a", "a", "b", "b", "a", "a"]
    #     }
    #   )
    #   df.group_by_dynamic(
    #     "time",
    #     every: "1h",
    #     closed: "both",
    #     by: "groups",
    #     include_boundaries: true
    #   ).agg([Polars.col("time").count.alias("time_count")])
    #   # =>
    #   # shape: (7, 5)
    #   # ┌────────┬─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
    #   # │ groups ┆ _lower_boundary     ┆ _upper_boundary     ┆ time                ┆ time_count │
    #   # │ ---    ┆ ---                 ┆ ---                 ┆ ---                 ┆ ---        │
    #   # │ str    ┆ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        ┆ u32        │
    #   # ╞════════╪═════════════════════╪═════════════════════╪═════════════════════╪════════════╡
    #   # │ a      ┆ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ a      ┆ 2021-12-16 00:00:00 ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 00:00:00 ┆ 3          │
    #   # │ a      ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 1          │
    #   # │ a      ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 2          │
    #   # │ a      ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 04:00:00 ┆ 2021-12-16 03:00:00 ┆ 1          │
    #   # │ b      ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 2          │
    #   # │ b      ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 1          │
    #   # └────────┴─────────────────────┴─────────────────────┴─────────────────────┴────────────┘
    #
    # @example Dynamic group by on an index column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "idx" => Polars.arange(0, 6, eager: true),
    #       "A" => ["A", "A", "B", "B", "B", "C"]
    #     }
    #   )
    #   df.group_by_dynamic(
    #     "idx",
    #     every: "2i",
    #     period: "3i",
    #     include_boundaries: true,
    #     closed: "right"
    #   ).agg(Polars.col("A").alias("A_agg_list"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────────────────┬─────────────────┬─────┬─────────────────┐
    #   # │ _lower_boundary ┆ _upper_boundary ┆ idx ┆ A_agg_list      │
    #   # │ ---             ┆ ---             ┆ --- ┆ ---             │
    #   # │ i64             ┆ i64             ┆ i64 ┆ list[str]       │
    #   # ╞═════════════════╪═════════════════╪═════╪═════════════════╡
    #   # │ -2              ┆ 1               ┆ -2  ┆ ["A", "A"]      │
    #   # │ 0               ┆ 3               ┆ 0   ┆ ["A", "B", "B"] │
    #   # │ 2               ┆ 5               ┆ 2   ┆ ["B", "B", "C"] │
    #   # │ 4               ┆ 7               ┆ 4   ┆ ["C"]           │
    #   # └─────────────────┴─────────────────┴─────┴─────────────────┘
    def group_by_dynamic(
      index_column,
      every:,
      period: nil,
      offset: nil,
      truncate: nil,
      include_boundaries: false,
      closed: "left",
      label: "left",
      by: nil,
      start_by: "window"
    )
      if !truncate.nil?
        label = truncate ? "left" : "datapoint"
      end

      index_column = Utils.parse_into_expression(index_column, str_as_lit: false)
      if offset.nil?
        offset = period.nil? ? "-#{every}" : "0ns"
      end

      if period.nil?
        period = every
      end

      period = Utils.parse_as_duration_string(period)
      offset = Utils.parse_as_duration_string(offset)
      every = Utils.parse_as_duration_string(every)

      rbexprs_by = by.nil? ? [] : Utils.parse_into_list_of_expressions(by)
      lgb = _ldf.group_by_dynamic(
        index_column,
        every,
        period,
        offset,
        label,
        include_boundaries,
        closed,
        rbexprs_by,
        start_by
      )
      LazyGroupBy.new(lgb)
    end
    alias_method :groupby_dynamic, :group_by_dynamic

    # Perform an asof join.
    #
    # This is similar to a left-join except that we match on nearest key rather than
    # equal keys.
    #
    # Both DataFrames must be sorted by the join_asof key.
    #
    # For each row in the left DataFrame:
    #
    # - A "backward" search selects the last row in the right DataFrame whose 'on' key is less than or equal to the left's key.
    # - A "forward" search selects the first row in the right DataFrame whose 'on' key is greater than or equal to the left's key.
    #
    # The default is "backward".
    #
    # @param other [LazyFrame]
    #   Lazy DataFrame to join with.
    # @param left_on [String]
    #   Join column of the left DataFrame.
    # @param right_on [String]
    #   Join column of the right DataFrame.
    # @param on [String]
    #   Join column of both DataFrames. If set, `left_on` and `right_on` should be
    #   None.
    # @param by [Object]
    #   Join on these columns before doing asof join.
    # @param by_left [Object]
    #   Join on these columns before doing asof join.
    # @param by_right [Object]
    #   Join on these columns before doing asof join.
    # @param strategy ["backward", "forward"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param tolerance [Object]
    #   Numeric tolerance. By setting this the join will only be done if the near
    #   keys are within this distance. If an asof join is done on columns of dtype
    #   "Date", "Datetime", "Duration" or "Time" you use the following string
    #   language:
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
    #   Or combine them:
    #   "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    # @param coalesce [Boolean]
    #   Coalescing behavior (merging of join columns).
    #     - true: -> Always coalesce join columns.
    #     - false: -> Never coalesce join columns.
    #   Note that joining on any other expressions than `col` will turn off coalescing.
    # @param allow_exact_matches [Boolean]
    #   Whether exact matches are valid join predicates.
    #     - If true, allow matching with the same `on` value (i.e. less-than-or-equal-to / greater-than-or-equal-to).
    #     - If false, don't match the same `on` value (i.e., strictly less-than / strictly greater-than).
    # @param check_sortedness [Boolean]
    #   Check the sortedness of the asof keys. If the keys are not sorted Polars
    #   will error, or in case of 'by' argument raise a warning. This might become
    #   a hard error in the future.
    #
    # @return [LazyFrame]
    #
    # @example
    #   gdp = Polars::LazyFrame.new(
    #     {
    #       "date" => Polars.date_range(
    #         Date.new(2016, 1, 1),
    #         Date.new(2020, 1, 1),
    #         "1y",
    #         eager: true
    #       ),
    #       "gdp" => [4164, 4411, 4566, 4696, 4827]
    #     }
    #   )
    #   gdp.collect
    #   # =>
    #   # shape: (5, 2)
    #   # ┌────────────┬──────┐
    #   # │ date       ┆ gdp  │
    #   # │ ---        ┆ ---  │
    #   # │ date       ┆ i64  │
    #   # ╞════════════╪══════╡
    #   # │ 2016-01-01 ┆ 4164 │
    #   # │ 2017-01-01 ┆ 4411 │
    #   # │ 2018-01-01 ┆ 4566 │
    #   # │ 2019-01-01 ┆ 4696 │
    #   # │ 2020-01-01 ┆ 4827 │
    #   # └────────────┴──────┘
    #
    # @example
    #   population = Polars::LazyFrame.new(
    #     {
    #       "date" => [Date.new(2016, 3, 1), Date.new(2018, 8, 1), Date.new(2019, 1, 1)],
    #       "population" => [82.19, 82.66, 83.12]
    #     }
    #   ).sort("date")
    #   population.collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬────────────┐
    #   # │ date       ┆ population │
    #   # │ ---        ┆ ---        │
    #   # │ date       ┆ f64        │
    #   # ╞════════════╪════════════╡
    #   # │ 2016-03-01 ┆ 82.19      │
    #   # │ 2018-08-01 ┆ 82.66      │
    #   # │ 2019-01-01 ┆ 83.12      │
    #   # └────────────┴────────────┘
    #
    # @example Note how the dates don't quite match. If we join them using `join_asof` and `strategy: "backward"`, then each date from `population` which doesn't have an exact match is matched with the closest earlier date from `gdp`:
    #   population.join_asof(gdp, on: "date", strategy: "backward").collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬────────────┬──────┐
    #   # │ date       ┆ population ┆ gdp  │
    #   # │ ---        ┆ ---        ┆ ---  │
    #   # │ date       ┆ f64        ┆ i64  │
    #   # ╞════════════╪════════════╪══════╡
    #   # │ 2016-03-01 ┆ 82.19      ┆ 4164 │
    #   # │ 2018-08-01 ┆ 82.66      ┆ 4566 │
    #   # │ 2019-01-01 ┆ 83.12      ┆ 4696 │
    #   # └────────────┴────────────┴──────┘
    #
    # @example
    #   population.join_asof(
    #     gdp, on: "date", strategy: "backward", coalesce: false
    #   ).collect
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────────┬────────────┬────────────┬──────┐
    #   # │ date       ┆ population ┆ date_right ┆ gdp  │
    #   # │ ---        ┆ ---        ┆ ---        ┆ ---  │
    #   # │ date       ┆ f64        ┆ date       ┆ i64  │
    #   # ╞════════════╪════════════╪════════════╪══════╡
    #   # │ 2016-03-01 ┆ 82.19      ┆ 2016-01-01 ┆ 4164 │
    #   # │ 2018-08-01 ┆ 82.66      ┆ 2018-01-01 ┆ 4566 │
    #   # │ 2019-01-01 ┆ 83.12      ┆ 2019-01-01 ┆ 4696 │
    #   # └────────────┴────────────┴────────────┴──────┘
    #
    # @example If we instead use `strategy: "forward"`, then each date from `population` which doesn't have an exact match is matched with the closest later date from `gdp`:
    #   population.join_asof(gdp, on: "date", strategy: "forward").collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬────────────┬──────┐
    #   # │ date       ┆ population ┆ gdp  │
    #   # │ ---        ┆ ---        ┆ ---  │
    #   # │ date       ┆ f64        ┆ i64  │
    #   # ╞════════════╪════════════╪══════╡
    #   # │ 2016-03-01 ┆ 82.19      ┆ 4411 │
    #   # │ 2018-08-01 ┆ 82.66      ┆ 4696 │
    #   # │ 2019-01-01 ┆ 83.12      ┆ 4696 │
    #   # └────────────┴────────────┴──────┘
    #
    # @example
    #   population.join_asof(gdp, on: "date", strategy: "nearest").collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────────┬────────────┬──────┐
    #   # │ date       ┆ population ┆ gdp  │
    #   # │ ---        ┆ ---        ┆ ---  │
    #   # │ date       ┆ f64        ┆ i64  │
    #   # ╞════════════╪════════════╪══════╡
    #   # │ 2016-03-01 ┆ 82.19      ┆ 4164 │
    #   # │ 2018-08-01 ┆ 82.66      ┆ 4696 │
    #   # │ 2019-01-01 ┆ 83.12      ┆ 4696 │
    #   # └────────────┴────────────┴──────┘
    #
    # @example
    #   gdp_dates = Polars.date_range(
    #     Date.new(2016, 1, 1), Date.new(2020, 1, 1), "1y", eager: true
    #   )
    #   gdp2 = Polars::LazyFrame.new(
    #     {
    #       "country" => ["Germany"] * 5 + ["Netherlands"] * 5,
    #       "date" => Polars.concat([gdp_dates, gdp_dates]),
    #       "gdp" => [4164, 4411, 4566, 4696, 4827, 784, 833, 914, 910, 909]
    #     }
    #   ).sort("country", "date")
    #   gdp2.collect
    #   # =>
    #   # shape: (10, 3)
    #   # ┌─────────────┬────────────┬──────┐
    #   # │ country     ┆ date       ┆ gdp  │
    #   # │ ---         ┆ ---        ┆ ---  │
    #   # │ str         ┆ date       ┆ i64  │
    #   # ╞═════════════╪════════════╪══════╡
    #   # │ Germany     ┆ 2016-01-01 ┆ 4164 │
    #   # │ Germany     ┆ 2017-01-01 ┆ 4411 │
    #   # │ Germany     ┆ 2018-01-01 ┆ 4566 │
    #   # │ Germany     ┆ 2019-01-01 ┆ 4696 │
    #   # │ Germany     ┆ 2020-01-01 ┆ 4827 │
    #   # │ Netherlands ┆ 2016-01-01 ┆ 784  │
    #   # │ Netherlands ┆ 2017-01-01 ┆ 833  │
    #   # │ Netherlands ┆ 2018-01-01 ┆ 914  │
    #   # │ Netherlands ┆ 2019-01-01 ┆ 910  │
    #   # │ Netherlands ┆ 2020-01-01 ┆ 909  │
    #   # └─────────────┴────────────┴──────┘
    #
    # @example
    #   pop2 = Polars::LazyFrame.new(
    #     {
    #       "country" => ["Germany"] * 3 + ["Netherlands"] * 3,
    #       "date" => [
    #         Date.new(2016, 3, 1),
    #         Date.new(2018, 8, 1),
    #         Date.new(2019, 1, 1),
    #         Date.new(2016, 3, 1),
    #         Date.new(2018, 8, 1),
    #         Date.new(2019, 1, 1)
    #       ],
    #       "population" => [82.19, 82.66, 83.12, 17.11, 17.32, 17.40]
    #     }
    #   ).sort("country", "date")
    #   pop2.collect
    #   # =>
    #   # shape: (6, 3)
    #   # ┌─────────────┬────────────┬────────────┐
    #   # │ country     ┆ date       ┆ population │
    #   # │ ---         ┆ ---        ┆ ---        │
    #   # │ str         ┆ date       ┆ f64        │
    #   # ╞═════════════╪════════════╪════════════╡
    #   # │ Germany     ┆ 2016-03-01 ┆ 82.19      │
    #   # │ Germany     ┆ 2018-08-01 ┆ 82.66      │
    #   # │ Germany     ┆ 2019-01-01 ┆ 83.12      │
    #   # │ Netherlands ┆ 2016-03-01 ┆ 17.11      │
    #   # │ Netherlands ┆ 2018-08-01 ┆ 17.32      │
    #   # │ Netherlands ┆ 2019-01-01 ┆ 17.4       │
    #   # └─────────────┴────────────┴────────────┘
    #
    # @example
    #   pop2.join_asof(gdp2, by: "country", on: "date", strategy: "nearest").collect
    #   # =>
    #   # shape: (6, 4)
    #   # ┌─────────────┬────────────┬────────────┬──────┐
    #   # │ country     ┆ date       ┆ population ┆ gdp  │
    #   # │ ---         ┆ ---        ┆ ---        ┆ ---  │
    #   # │ str         ┆ date       ┆ f64        ┆ i64  │
    #   # ╞═════════════╪════════════╪════════════╪══════╡
    #   # │ Germany     ┆ 2016-03-01 ┆ 82.19      ┆ 4164 │
    #   # │ Germany     ┆ 2018-08-01 ┆ 82.66      ┆ 4696 │
    #   # │ Germany     ┆ 2019-01-01 ┆ 83.12      ┆ 4696 │
    #   # │ Netherlands ┆ 2016-03-01 ┆ 17.11      ┆ 784  │
    #   # │ Netherlands ┆ 2018-08-01 ┆ 17.32      ┆ 910  │
    #   # │ Netherlands ┆ 2019-01-01 ┆ 17.4       ┆ 910  │
    #   # └─────────────┴────────────┴────────────┴──────┘
    def join_asof(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      by_left: nil,
      by_right: nil,
      by: nil,
      strategy: "backward",
      suffix: "_right",
      tolerance: nil,
      allow_parallel: true,
      force_parallel: false,
      coalesce: true,
      allow_exact_matches: true,
      check_sortedness: true
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if on.is_a?(::String)
        left_on = on
        right_on = on
      end

      if left_on.nil? || right_on.nil?
        raise ArgumentError, "You should pass the column to join on as an argument."
      end

      if by_left.is_a?(::String) || by_left.is_a?(Expr)
        by_left_ = [by_left]
      else
        by_left_ = by_left
      end

      if by_right.is_a?(::String) || by_right.is_a?(Expr)
        by_right_ = [by_right]
      else
        by_right_ = by_right
      end

      if by.is_a?(::String)
        by_left_ = [by]
        by_right_ = [by]
      elsif by.is_a?(::Array)
        by_left_ = by
        by_right_ = by
      end

      tolerance_str = nil
      tolerance_num = nil
      if tolerance.is_a?(::String)
        tolerance_str = tolerance
      else
        tolerance_num = tolerance
      end

      _from_rbldf(
        _ldf.join_asof(
          other._ldf,
          Polars.col(left_on)._rbexpr,
          Polars.col(right_on)._rbexpr,
          by_left_,
          by_right_,
          allow_parallel,
          force_parallel,
          suffix,
          strategy,
          tolerance_num,
          tolerance_str,
          coalesce,
          allow_exact_matches,
          check_sortedness
        )
      )
    end

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
    # @param how ["inner", "left", "full", "semi", "anti", "cross"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param validate ['m:m', 'm:1', '1:m', '1:1']
    #   Checks if join is of specified type.
    #     * *many_to_many* - “m:m”: default, does not result in checks
    #     * *one_to_one* - “1:1”: check if join keys are unique in both left and right datasets
    #     * *one_to_many* - “1:m”: check if join keys are unique in left dataset
    #     * *many_to_one* - “m:1”: check if join keys are unique in right dataset
    # @param join_nulls [Boolean]
    #   Join on null values. By default null values will never produce matches.
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    # @param coalesce [Boolean]
    #   Coalescing behavior (merging of join columns).
    #     - nil: -> join specific.
    #     - true: -> Always coalesce join columns.
    #     - false: -> Never coalesce join columns.
    #   Note that joining on any other expressions than `col` will turn off coalescing.
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
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "full").collect
    #   # =>
    #   # shape: (4, 5)
    #   # ┌──────┬──────┬──────┬───────┬───────────┐
    #   # │ foo  ┆ bar  ┆ ham  ┆ apple ┆ ham_right │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---   ┆ ---       │
    #   # │ i64  ┆ f64  ┆ str  ┆ str   ┆ str       │
    #   # ╞══════╪══════╪══════╪═══════╪═══════════╡
    #   # │ 1    ┆ 6.0  ┆ a    ┆ x     ┆ a         │
    #   # │ 2    ┆ 7.0  ┆ b    ┆ y     ┆ b         │
    #   # │ null ┆ null ┆ null ┆ z     ┆ d         │
    #   # │ 3    ┆ 8.0  ┆ c    ┆ null  ┆ null      │
    #   # └──────┴──────┴──────┴───────┴───────────┘
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
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
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
      validate: "m:m",
      join_nulls: false,
      allow_parallel: true,
      force_parallel: false,
      coalesce: nil
    )
      if !other.is_a?(LazyFrame)
        raise ArgumentError, "Expected a `LazyFrame` as join table, got #{other.class.name}"
      end

      if how == "outer"
        how = "full"
      elsif how == "cross"
        return _from_rbldf(
          _ldf.join(
            other._ldf, [], [], allow_parallel, join_nulls, force_parallel, how, suffix, validate, coalesce
          )
        )
      end

      if !on.nil?
        rbexprs = Utils.parse_into_list_of_expressions(on)
        rbexprs_left = rbexprs
        rbexprs_right = rbexprs
      elsif !left_on.nil? && !right_on.nil?
        rbexprs_left = Utils.parse_into_list_of_expressions(left_on)
        rbexprs_right = Utils.parse_into_list_of_expressions(right_on)
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
          join_nulls,
          how,
          suffix,
          validate,
          coalesce
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
    #   # ┌─────┬──────┬───────┬─────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2 ┆ b/2  ┆ not c │
    #   # │ --- ┆ ---  ┆ ---   ┆ --- ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪═════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1   ┆ 0.25 ┆ false │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4   ┆ 2.0  ┆ false │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9   ┆ 5.0  ┆ true  │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16  ┆ 6.5  ┆ false │
    #   # └─────┴──────┴───────┴─────┴──────┴───────┘
    def with_columns(*exprs, **named_exprs)
      structify = ENV.fetch("POLARS_AUTO_STRUCTIFY", "0") != "0"

      rbexprs = Utils.parse_into_list_of_expressions(*exprs, **named_exprs, __structify: structify)

      _from_rbldf(_ldf.with_columns(rbexprs))
    end

    # Add an external context to the computation graph.
    #
    # This allows expressions to also access columns from DataFrames
    # that are not part of this one.
    #
    # @param other [Object]
    #   Lazy DataFrame to join with.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df_a = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["a", "c", nil]}).lazy
    #   df_other = Polars::DataFrame.new({"c" => ["foo", "ham"]})
    #   (
    #     df_a.with_context(df_other.lazy).select(
    #       [Polars.col("b") + Polars.col("c").first]
    #     )
    #   ).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌──────┐
    #   # │ b    │
    #   # │ ---  │
    #   # │ str  │
    #   # ╞══════╡
    #   # │ afoo │
    #   # │ cfoo │
    #   # │ null │
    #   # └──────┘
    def with_context(other)
      if !other.is_a?(::Array)
        other = [other]
      end

      _from_rbldf(_ldf.with_context(other.map(&:_ldf)))
    end

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
    #   # │ i64 ┆ i64 ┆ i64       │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ 2   ┆ 4         │
    #   # │ 3   ┆ 4   ┆ 16        │
    #   # │ 5   ┆ 6   ┆ 36        │
    #   # └─────┴─────┴───────────┘
    #
    # @example
    #   df.with_column(Polars.col("a") ** 2).collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 2   │
    #   # │ 9   ┆ 4   │
    #   # │ 25  ┆ 6   │
    #   # └─────┴─────┘
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
    #
    # @example Drop a single column by passing the name of that column.
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   lf.drop("ham").collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6.0 │
    #   # │ 2   ┆ 7.0 │
    #   # │ 3   ┆ 8.0 │
    #   # └─────┴─────┘
    #
    # @example Drop multiple columns by passing a selector.
    #   lf.drop(Polars.cs.numeric).collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ ham │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ a   │
    #   # │ b   │
    #   # │ c   │
    #   # └─────┘
    #
    # @example Use positional arguments to drop multiple columns.
    #   lf.drop("foo", "ham").collect
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ bar │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 6.0 │
    #   # │ 7.0 │
    #   # │ 8.0 │
    #   # └─────┘
    def drop(*columns)
      drop_cols = Utils._expand_selectors(self, *columns)
      _from_rbldf(_ldf.drop(drop_cols))
    end

    # Rename column names.
    #
    # @param mapping [Hash]
    #   Key value pairs that map from old name to new name.
    # @param strict [Boolean]
    #   Validate that all column names exist in the current schema,
    #   and throw an exception if any do not. (Note that this parameter
    #   is a no-op when passing a function to `mapping`).
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   lf.rename({"foo" => "apple"}).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ apple ┆ bar ┆ ham │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ i64   ┆ i64 ┆ str │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 1     ┆ 6   ┆ a   │
    #   # │ 2     ┆ 7   ┆ b   │
    #   # │ 3     ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    #
    # @example
    #   lf.rename(->(column_name) { "c" + column_name[1..] }).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ coo ┆ car ┆ cam │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def rename(mapping, strict: true)
      if mapping.respond_to?(:call)
        select(F.all.name.map(&mapping))
      else
        existing = mapping.keys
        _new = mapping.values
        _from_rbldf(_ldf.rename(existing, _new, strict))
      end
    end

    # Reverse the DataFrame.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "key" => ["a", "b", "c"],
    #       "val" => [1, 2, 3]
    #     }
    #   )
    #   lf.reverse.collect
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ key ┆ val │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ c   ┆ 3   │
    #   # │ b   ┆ 2   │
    #   # │ a   ┆ 1   │
    #   # └─────┴─────┘
    def reverse
      _from_rbldf(_ldf.reverse)
    end

    # Shift the values by a given period.
    #
    # @param n [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill the resulting null values with this value.
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
    #   # │ 1    ┆ 2    │
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
    #   # │ 5    ┆ 6    │
    #   # │ null ┆ null │
    #   # └──────┴──────┘
    def shift(n, fill_value: nil)
      if !fill_value.nil?
        fill_value = Utils.parse_into_expression(fill_value, str_as_lit: true)
      end
      n = Utils.parse_into_expression(n)
      _from_rbldf(_ldf.shift(n, fill_value))
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
    #   # │ 1   ┆ 2   │
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
    #   # │ 5   ┆ 6   │
    #   # │ 0   ┆ 0   │
    #   # └─────┴─────┘
    def shift_and_fill(periods, fill_value)
      shift(periods, fill_value: fill_value)
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
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4, 5, 6],
    #       "b" => [7, 8, 9, 10, 11, 12]
    #     }
    #   )
    #   lf.limit.collect
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 7   │
    #   # │ 2   ┆ 8   │
    #   # │ 3   ┆ 9   │
    #   # │ 4   ┆ 10  │
    #   # │ 5   ┆ 11  │
    #   # └─────┴─────┘
    #
    # @example
    #   lf.limit(2).collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 7   │
    #   # │ 2   ┆ 8   │
    #   # └─────┴─────┘
    def limit(n = 5)
      head(n)
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
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4, 5, 6],
    #       "b" => [7, 8, 9, 10, 11, 12]
    #     }
    #   )
    #   lf.head.collect
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 7   │
    #   # │ 2   ┆ 8   │
    #   # │ 3   ┆ 9   │
    #   # │ 4   ┆ 10  │
    #   # │ 5   ┆ 11  │
    #   # └─────┴─────┘
    #
    # @example
    #   lf.head(2).collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 7   │
    #   # │ 2   ┆ 8   │
    #   # └─────┴─────┘
    def head(n = 5)
      slice(0, n)
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #     Number of rows.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4, 5, 6],
    #       "b" => [7, 8, 9, 10, 11, 12]
    #     }
    #   )
    #   lf.tail.collect
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 8   │
    #   # │ 3   ┆ 9   │
    #   # │ 4   ┆ 10  │
    #   # │ 5   ┆ 11  │
    #   # │ 6   ┆ 12  │
    #   # └─────┴─────┘
    #
    # @example
    #   lf.tail(2).collect
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 5   ┆ 11  │
    #   # │ 6   ┆ 12  │
    #   # └─────┴─────┘
    def tail(n = 5)
      _from_rbldf(_ldf.tail(n))
    end

    # Get the last row of the DataFrame.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 5, 3],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   lf.last.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 3   ┆ 6   │
    #   # └─────┴─────┘
    def last
      tail(1)
    end

    # Get the first row of the DataFrame.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 5, 3],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   lf.first.collect
    #   # =>
    #   # shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 2   │
    #   # └─────┴─────┘
    def first
      slice(0, 1)
    end

    # Add a column at index 0 that counts the rows.
    #
    # @param name [String]
    #   Name of the column to add.
    # @param offset [Integer]
    #   Start the row count at this offset.
    #
    # @return [LazyFrame]
    #
    # @note
    #   This can have a negative effect on query performance.
    #   This may, for instance, block predicate pushdown optimization.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   ).lazy
    #   df.with_row_index.collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ index ┆ a   ┆ b   │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ u32   ┆ i64 ┆ i64 │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 0     ┆ 1   ┆ 2   │
    #   # │ 1     ┆ 3   ┆ 4   │
    #   # │ 2     ┆ 5   ┆ 6   │
    #   # └───────┴─────┴─────┘
    def with_row_index(name: "index", offset: 0)
      _from_rbldf(_ldf.with_row_index(name, offset))
    end
    alias_method :with_row_count, :with_row_index

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
    #   # │ 3   ┆ 7   │
    #   # └─────┴─────┘
    def take_every(n)
      select(F.col("*").take_every(n))
    end

    # Fill null values using the specified value or strategy.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => [1, 2, nil, 4],
    #       "b" => [0.5, 4, nil, 13]
    #     }
    #   )
    #   lf.fill_null(99).collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 99  ┆ 99.0 │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   lf.fill_null(strategy: "forward").collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   lf.fill_null(strategy: "max").collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 4   ┆ 13.0 │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   lf.fill_null(strategy: "zero").collect
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 0   ┆ 0.0  │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    def fill_null(value = nil, strategy: nil, limit: nil, matches_supertype: nil)
      select(Polars.all.fill_null(value, strategy: strategy, limit: limit))
    end

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
    #   # │ 2.0  ┆ 4.0  │
    #   # │ 99.0 ┆ 99.0 │
    #   # │ 4.0  ┆ 13.0 │
    #   # └──────┴──────┘
    def fill_nan(fill_value)
      if !fill_value.is_a?(Expr)
        fill_value = F.lit(fill_value)
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
      quantile = Utils.parse_into_expression(quantile, str_as_lit: false)
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
    #   # │ a       ┆ 2       │
    #   # │ a       ┆ 3       │
    #   # │ b       ┆ 4       │
    #   # │ b       ┆ 5       │
    #   # │ c       ┆ 6       │
    #   # │ c       ┆ 7       │
    #   # │ c       ┆ 8       │
    #   # └─────────┴─────────┘
    def explode(columns)
      columns = Utils.parse_into_list_of_expressions(columns)
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
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 1],
    #       "bar" => ["a", "a", "a", "a"],
    #       "ham" => ["b", "b", "b", "b"]
    #     }
    #   )
    #   lf.unique(maintain_order: true).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ str ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ a   ┆ b   │
    #   # │ 2   ┆ a   ┆ b   │
    #   # │ 3   ┆ a   ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   lf.unique(subset: ["bar", "ham"], maintain_order: true).collect
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ str ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ a   ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   lf.unique(keep: "last", maintain_order: true).collect
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ str ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 2   ┆ a   ┆ b   │
    #   # │ 3   ┆ a   ┆ b   │
    #   # │ 1   ┆ a   ┆ b   │
    #   # └─────┴─────┴─────┘
    def unique(maintain_order: true, subset: nil, keep: "first")
      if !subset.nil? && !subset.is_a?(::Array)
        subset = [subset]
      end
      _from_rbldf(_ldf.unique(maintain_order, subset, keep))
    end

    # Drop rows with null values from this LazyFrame.
    #
    # @param subset [Object]
    #   Subset of column(s) on which `drop_nulls` will be applied.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, nil, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.lazy.drop_nulls.collect
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def drop_nulls(subset: nil)
      if !subset.nil? && !subset.is_a?(::Array)
        subset = [subset]
      end
      _from_rbldf(_ldf.drop_nulls(subset))
    end

    # Unpivot a DataFrame from wide to long format.
    #
    # Optionally leaves identifiers set.
    #
    # This function is useful to massage a DataFrame into a format where one or more
    # columns are identifier variables (index) while all other columns, considered
    # measured variables (on), are "unpivoted" to the row axis leaving just
    # two non-identifier columns, 'variable' and 'value'.
    #
    # @param on [Object]
    #   Column(s) or selector(s) to use as values variables; if `on`
    #   is empty all columns that are not in `index` will be used.
    # @param index [Object]
    #   Column(s) or selector(s) to use as identifier variables.
    # @param variable_name [String]
    #   Name to give to the `variable` column. Defaults to "variable"
    # @param value_name [String]
    #   Name to give to the `value` column. Defaults to "value"
    # @param streamable [Boolean]
    #   Allow this node to run in the streaming engine.
    #   If this runs in streaming, the output of the unpivot operation
    #   will not have a stable ordering.
    #
    # @return [LazyFrame]
    #
    # @example
    #   lf = Polars::LazyFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "b" => [1, 3, 5],
    #       "c" => [2, 4, 6]
    #     }
    #   )
    #   lf.unpivot(Polars.cs.numeric, index: "a").collect
    #   # =>
    #   # shape: (6, 3)
    #   # ┌─────┬──────────┬───────┐
    #   # │ a   ┆ variable ┆ value │
    #   # │ --- ┆ ---      ┆ ---   │
    #   # │ str ┆ str      ┆ i64   │
    #   # ╞═════╪══════════╪═══════╡
    #   # │ x   ┆ b        ┆ 1     │
    #   # │ y   ┆ b        ┆ 3     │
    #   # │ z   ┆ b        ┆ 5     │
    #   # │ x   ┆ c        ┆ 2     │
    #   # │ y   ┆ c        ┆ 4     │
    #   # │ z   ┆ c        ┆ 6     │
    #   # └─────┴──────────┴───────┘
    def unpivot(
      on,
      index: nil,
      variable_name: nil,
      value_name: nil,
      streamable: true
    )
      if !streamable
        warn "The `streamable` parameter for `LazyFrame.unpivot` is deprecated"
      end

      on = on.nil? ? [] : Utils.parse_into_list_of_expressions(on)
      index = index.nil? ? [] : Utils.parse_into_list_of_expressions(index)

      _from_rbldf(
        _ldf.unpivot(on, index, value_name, variable_name)
      )
    end
    alias_method :melt, :unpivot

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
    #   # ┌──────┬──────┬──────────┐
    #   # │ foo  ┆ bar  ┆ baz      │
    #   # │ ---  ┆ ---  ┆ ---      │
    #   # │ f64  ┆ f64  ┆ f64      │
    #   # ╞══════╪══════╪══════════╡
    #   # │ 1.0  ┆ 6.0  ┆ 1.0      │
    #   # │ 5.0  ┆ 7.0  ┆ 3.666667 │
    #   # │ 9.0  ┆ 9.0  ┆ 6.333333 │
    #   # │ 10.0 ┆ null ┆ 9.0      │
    #   # └──────┴──────┴──────────┘
    def interpolate
      select(F.col("*").interpolate)
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
    #   # │ bar    ┆ 2   ┆ b   ┆ null ┆ [3]       ┆ womp  │
    #   # └────────┴─────┴─────┴──────┴───────────┴───────┘
    def unnest(names)
      if names.is_a?(::String)
        names = [names]
      end
      _from_rbldf(_ldf.unnest(names))
    end

    # Take two sorted DataFrames and merge them by the sorted key.
    #
    # The output of this operation will also be sorted.
    # It is the callers responsibility that the frames are sorted
    # by that key otherwise the output will not make sense.
    #
    # The schemas of both LazyFrames must be equal.
    #
    # @param other [DataFrame]
    #   Other DataFrame that must be merged
    # @param key [String]
    #   Key that is sorted.
    #
    # @return [LazyFrame]
    #
    # @example
    #   df0 = Polars::LazyFrame.new(
    #     {"name" => ["steve", "elise", "bob"], "age" => [42, 44, 18]}
    #   ).sort("age")
    #   df1 = Polars::LazyFrame.new(
    #     {"name" => ["anna", "megan", "steve", "thomas"], "age" => [21, 33, 42, 20]}
    #   ).sort("age")
    #   df0.merge_sorted(df1, "age").collect
    #   # =>
    #   # shape: (7, 2)
    #   # ┌────────┬─────┐
    #   # │ name   ┆ age │
    #   # │ ---    ┆ --- │
    #   # │ str    ┆ i64 │
    #   # ╞════════╪═════╡
    #   # │ bob    ┆ 18  │
    #   # │ thomas ┆ 20  │
    #   # │ anna   ┆ 21  │
    #   # │ megan  ┆ 33  │
    #   # │ steve  ┆ 42  │
    #   # │ steve  ┆ 42  │
    #   # │ elise  ┆ 44  │
    #   # └────────┴─────┘
    def merge_sorted(other, key)
      _from_rbldf(_ldf.merge_sorted(other._ldf, key))
    end

    # Flag a column as sorted.
    #
    # This can speed up future operations.
    #
    # @note
    #   This can lead to incorrect results if the data is NOT sorted! Use with care!
    #
    # @param column [Object]
    #   Column that is sorted.
    # @param descending [Boolean]
    #   Whether the column is sorted in descending order.
    #
    # @return [LazyFrame]
    def set_sorted(
      column,
      descending: false
    )
      if !Utils.strlike?(column)
        msg = "expected a 'str' for argument 'column' in 'set_sorted'"
        raise TypeError, msg
      end
      with_columns(F.col(column).set_sorted(descending: descending))
    end

    # TODO
    # def update
    # end

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

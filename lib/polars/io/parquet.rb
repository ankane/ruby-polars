module Polars
  module IO
    # Read into a DataFrame from a parquet file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param parallel ["auto", "columns", "row_groups", "none"]
    #   This determines the direction of parallelism. 'auto' will try to determine the
    #   optimal direction.
    # @param use_statistics [Boolean]
    #   Use statistics in the parquet to determine if pages
    #   can be skipped from reading.
    # @param hive_partitioning [Boolean]
    #   Infer statistics and schema from hive partitioned URL and use them
    #   to prune reads.
    # @param glob [Boolean]
    #   Expand path given via globbing rules.
    # @param schema [Object]
    #   Specify the datatypes of the columns. The datatypes must match the
    #   datatypes in the file(s). If there are extra columns that are not in the
    #   file(s), consider also enabling `allow_missing_columns`.
    # @param hive_schema [Object]
    #   The column names and data types of the columns by which the data is partitioned.
    #   If set to `nil` (default), the schema of the Hive partitions is inferred.
    # @param try_parse_hive_dates [Boolean]
    #   Whether to try parsing hive values as date/datetime types.
    # @param rechunk [Boolean]
    #   In case of reading multiple files via a glob pattern rechunk the final DataFrame
    #   into contiguous memory chunks.
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param credential_provider [Object]
    #   Provide a function that can be called to provide cloud storage
    #   credentials. The function is expected to return a hash of
    #   credential keys along with an optional credential expiry time.
    # @param retries [Integer]
    #   Number of retries if accessing a cloud instance fails.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    # @param missing_columns ['insert', 'raise']
    #   Configuration for behavior when columns defined in the schema
    #   are missing from the data:
    #
    #   * `insert`: Inserts the missing columns using NULLs as the row values.
    #   * `raise`: Raises an error.
    # @param allow_missing_columns [Boolean]
    #   When reading a list of parquet files, if a column existing in the first
    #   file cannot be found in subsequent files, the default behavior is to
    #   raise an error. However, if `allow_missing_columns` is set to
    #   `true`, a full-NULL column is returned instead of erroring for the files
    #   that do not contain the column.
    #
    # @return [DataFrame]
    def read_parquet(
      source,
      columns: nil,
      n_rows: nil,
      row_count_name: nil,
      row_count_offset: 0,
      parallel: "auto",
      use_statistics: true,
      hive_partitioning: nil,
      glob: true,
      schema: nil,
      hive_schema: nil,
      try_parse_hive_dates: true,
      rechunk: false,
      low_memory: false,
      storage_options: nil,
      credential_provider: nil,
      retries: 2,
      include_file_paths: nil,
      missing_columns: "raise",
      allow_missing_columns: nil
    )
      lf =
        scan_parquet(
          source,
          n_rows: n_rows,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          parallel: parallel,
          use_statistics: use_statistics,
          hive_partitioning: hive_partitioning,
          schema: schema,
          hive_schema: hive_schema,
          try_parse_hive_dates: try_parse_hive_dates,
          rechunk: rechunk,
          low_memory: low_memory,
          cache: false,
          storage_options: storage_options,
          credential_provider: credential_provider,
          retries: retries,
          glob: glob,
          include_file_paths: include_file_paths,
          missing_columns: missing_columns,
          allow_missing_columns: allow_missing_columns
        )

      if !columns.nil?
        if Utils.is_int_sequence(columns)
          lf = lf.select(F.nth(columns))
        else
          lf = lf.select(columns)
        end
      end

      lf.collect
    end

    # Get a schema of the Parquet file without reading data.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [Schema]
    def read_parquet_schema(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      scan_parquet(source).collect_schema
    end

    # Get file-level custom metadata of a Parquet file without reading data.
    #
    # @note
    #   This functionality is considered **experimental**. It may be removed or
    #   changed at any point without it being considered a breaking change.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [Hash]
    def read_parquet_metadata(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source, check_not_directory: false)
      end

      Plr.read_parquet_metadata(source)
    end

    # Lazily read from a parquet file or multiple files via glob patterns.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param parallel ["auto", "columns", "row_groups", "none"]
    #   This determines the direction of parallelism. 'auto' will try to determine the
    #   optimal direction.
    # @param use_statistics [Boolean]
    #   Use statistics in the parquet to determine if pages
    #   can be skipped from reading.
    # @param hive_partitioning [Boolean]
    #   Infer statistics and schema from hive partitioned URL and use them
    #   to prune reads.
    # @param glob [Boolean]
    #   Expand path given via globbing rules.
    # @param hidden_file_prefix [Boolean]
    #   Skip reading files whose names begin with the specified prefixes.
    # @param schema [Object]
    #   Specify the datatypes of the columns. The datatypes must match the
    #   datatypes in the file(s). If there are extra columns that are not in the
    #   file(s), consider also enabling `allow_missing_columns`.
    # @param hive_schema [Object]
    #   The column names and data types of the columns by which the data is partitioned.
    #   If set to `nil` (default), the schema of the Hive partitions is inferred.
    # @param try_parse_hive_dates [Boolean]
    #   Whether to try parsing hive values as date/datetime types.
    # @param rechunk [Boolean]
    #   In case of reading multiple files via a glob pattern rechunk the final DataFrame
    #   into contiguous memory chunks.
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    # @param cache [Boolean]
    #   Cache the result after reading.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param credential_provider [Object]
    #   Provide a function that can be called to provide cloud storage
    #   credentials. The function is expected to return a hash of
    #   credential keys along with an optional credential expiry time.
    # @param retries [Integer]
    #   Number of retries if accessing a cloud instance fails.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    # @param missing_columns ['insert', 'raise']
    #   Configuration for behavior when columns defined in the schema
    #   are missing from the data:
    #
    #   * `insert`: Inserts the missing columns using NULLs as the row values.
    #   * `raise`: Raises an error.
    # @param allow_missing_columns [Boolean]
    #   When reading a list of parquet files, if a column existing in the first
    #   file cannot be found in subsequent files, the default behavior is to
    #   raise an error. However, if `allow_missing_columns` is set to
    #   `true`, a full-NULL column is returned instead of erroring for the files
    #   that do not contain the column.
    # @param extra_columns ['ignore', 'raise']
    #   Configuration for behavior when extra columns outside of the
    #   defined schema are encountered in the data:
    #     * `ignore`: Silently ignores.
    #     * `raise`: Raises an error.
    # @param cast_options [Object]
    #   Configuration for column type-casting during scans. Useful for datasets
    #   containing files that have differing schemas.
    #
    # @return [LazyFrame]
    def scan_parquet(
      source,
      n_rows: nil,
      row_count_name: nil,
      row_count_offset: 0,
      parallel: "auto",
      use_statistics: true,
      hive_partitioning: nil,
      glob: true,
      hidden_file_prefix: nil,
      schema: nil,
      hive_schema: nil,
      try_parse_hive_dates: true,
      rechunk: false,
      low_memory: false,
      cache: true,
      storage_options: nil,
      credential_provider: nil,
      retries: 2,
      include_file_paths: nil,
      missing_columns: "raise",
      allow_missing_columns: nil,
      extra_columns: "raise",
      cast_options: nil,
      _column_mapping: nil,
      _default_values: nil,
      _deletion_files: nil,
      _table_statistics: nil,
      _row_count: nil
    )
      row_index_name = row_count_name
      row_index_offset = row_count_offset

      if !schema.nil?
        msg = "the `schema` parameter of `scan_parquet` is considered unstable."
        Utils.issue_unstable_warning(msg)
      end

      if !hive_schema.nil?
        msg = "the `hive_schema` parameter of `scan_parquet` is considered unstable."
        Utils.issue_unstable_warning(msg)
      end

      if !cast_options.nil?
        msg = "The `cast_options` parameter of `scan_parquet` is considered unstable."
        Utils.issue_unstable_warning(msg)
      end

      if !hidden_file_prefix.nil?
        msg = "The `hidden_file_prefix` parameter of `scan_parquet` is considered unstable."
        Utils.issue_unstable_warning(msg)
      end

      if !allow_missing_columns.nil?
        Utils.issue_deprecation_warning(
          "the parameter `allow_missing_columns` for `scan_parquet` is deprecated. " +
          "Use the parameter `missing_columns` instead and pass one of " +
          "`('insert', 'raise')`."
        )

        missing_columns = allow_missing_columns ? "insert" : "raise"
      end

      sources = get_sources(source)

      credential_provider_builder =
        _init_credential_provider_builder(
          credential_provider,
          sources,
          storage_options,
          "scan_parquet"
        )

      rblf =
        RbLazyFrame.new_from_parquet(
          sources,
          schema,
          ScanOptions.new(
            row_index: !row_index_name.nil? ? [row_index_name, row_index_offset] : nil,
            pre_slice: !n_rows.nil? ? [0, n_rows] : nil,
            cast_options: cast_options,
            extra_columns: extra_columns,
            missing_columns: missing_columns,
            include_file_paths: include_file_paths,
            glob: glob,
            hidden_file_prefix: hidden_file_prefix.is_a?(::String) ? [hidden_file_prefix] : hidden_file_prefix,
            hive_partitioning: hive_partitioning,
            hive_schema: hive_schema,
            try_parse_hive_dates: try_parse_hive_dates,
            rechunk: rechunk,
            cache: cache,
            storage_options: storage_options ? storage_options.map { |k, v| [k.to_s, v.to_s] } : nil,
            credential_provider: credential_provider_builder,
            retries: retries,
            column_mapping: _column_mapping,
            default_values: _default_values,
            deletion_files: _deletion_files,
            table_statistics: _table_statistics,
            row_count: _row_count
          ),
          parallel,
          low_memory,
          use_statistics
        )
      Utils.wrap_ldf(rblf)
    end
  end
end

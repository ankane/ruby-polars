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
      allow_missing_columns: false
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
    # @return [Hash]
    def read_parquet_schema(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      # TODO return Schema
      scan_parquet(source).collect_schema.to_h
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
      allow_missing_columns: false,
      extra_columns: "raise"
    )
      missing_columns = allow_missing_columns ? "insert" : "raise"

      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source, check_not_directory: false)
      elsif Utils.is_path_or_str_sequence(source)
        source = source.map { |s| Utils.normalize_filepath(s, check_not_directory: false) }
      end

      if credential_provider
        raise Todo
      end

      if source.is_a?(::Array)
        sources = source
        source = nil
      else
        sources = [source]
      end

      if storage_options
        storage_options = storage_options.map { |k, v| [k.to_s, v.to_s] }
      else
        storage_options = nil
      end

      row_index_name = row_count_name
      row_index_offset = row_count_offset

      rblf =
        RbLazyFrame.new_from_parquet(
          sources,
          schema,
          ScanOptions.new(
            row_index: !row_index_name.nil? ? [row_index_name, row_index_offset] : nil,
            pre_slice: !n_rows.nil? ? [0, n_rows] : nil,
            # cast_options: cast_options,
            extra_columns: extra_columns,
            missing_columns: missing_columns,
            include_file_paths: include_file_paths,
            glob: glob,
            hive_partitioning: hive_partitioning,
            hive_schema: hive_schema,
            try_parse_hive_dates: try_parse_hive_dates,
            rechunk: rechunk,
            cache: cache,
            storage_options: storage_options,
            # credential_provider: credential_provider_builder,
            retries: retries,
            # deletion_files: _deletion_files
          ),
          parallel,
          low_memory,
          use_statistics
        )
      Utils.wrap_ldf(rblf)
    end
  end
end

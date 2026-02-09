module Polars
  module IO
    # Read into a DataFrame from Arrow IPC (Feather v2) file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from IPC file after reading `n_rows`.
    # @param memory_map [Boolean]
    #   Try to memory map the file. This can greatly improve performance on repeated
    #   queries as the OS may cache pages.
    #   Only uncompressed IPC files can be memory mapped.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param row_index_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_index_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param rechunk [Boolean]
    #   Make sure that all data is contiguous.
    #
    # @return [DataFrame]
    def read_ipc(
      source,
      columns: nil,
      n_rows: nil,
      memory_map: true,
      storage_options: nil,
      row_index_name: nil,
      row_index_offset: 0,
      rechunk: true
    )
      storage_options ||= {}
      _prepare_file_arg(source, **storage_options) do |data|
        _read_ipc_impl(
          data,
          columns: columns,
          n_rows: n_rows,
          row_index_name: row_index_name,
          row_index_offset: row_index_offset,
          rechunk: rechunk,
          memory_map: memory_map
        )
      end
    end

    # @private
    def _read_ipc_impl(
      file,
      columns: nil,
      n_rows: nil,
      row_index_name: nil,
      row_index_offset: 0,
      rechunk: true,
      memory_map: true
    )
      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end
      if columns.is_a?(::String)
        columns = [columns]
      end

      if file.is_a?(::String) && file.include?("*")
        raise Todo
      end

      projection, columns = Utils.handle_projection_columns(columns)
      rbdf =
        RbDataFrame.read_ipc(
          file,
          columns,
          projection,
          n_rows,
          Utils.parse_row_index_args(row_index_name, row_index_offset),
          memory_map
        )
      Utils.wrap_df(rbdf)
    end

    # Read into a DataFrame from Arrow IPC record batch stream.
    #
    # See "Streaming format" on https://arrow.apache.org/docs/python/ipc.html.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param columns [Array]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from IPC stream after reading `n_rows`.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param row_index_name [String]
    #   Insert a row index column with the given name into the DataFrame as the first
    #   column. If set to `nil` (default), no row index column is created.
    # @param row_index_offset [Integer]
    #   Start the row index at this offset. Cannot be negative.
    #   Only used if `row_index_name` is set.
    # @param rechunk [Boolean]
    #   Make sure that all data is contiguous.
    #
    # @return [DataFrame]
    def read_ipc_stream(
      source,
      columns: nil,
      n_rows: nil,
      storage_options: nil,
      row_index_name: nil,
      row_index_offset: 0,
      rechunk: true
    )
      storage_options ||= {}
      _prepare_file_arg(source, **storage_options) do |data|
        _read_ipc_stream_impl(
          data,
          columns: columns,
          n_rows: n_rows,
          row_index_name: row_index_name,
          row_index_offset: row_index_offset,
          rechunk: rechunk
        )
      end
    end

    # @private
    def _read_ipc_stream_impl(
      source,
      columns: nil,
      n_rows: nil,
      row_index_name: nil,
      row_index_offset: 0,
      rechunk: true
    )
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end
      if columns.is_a?(String)
        columns = [columns]
      end

      projection, columns = Utils.handle_projection_columns(columns)
      pydf = RbDataFrame.read_ipc_stream(
        source,
        columns,
        projection,
        n_rows,
        Utils.parse_row_index_args(row_index_name, row_index_offset),
        rechunk
      )
      Utils.wrap_df(pydf)
    end

    # Get a schema of the IPC file without reading data.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [Hash]
    def read_ipc_schema(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      Plr.ipc_schema(source)
    end

    # Lazily read from an Arrow IPC (Feather v2) file or multiple files via glob patterns.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a IPC file.
    # @param n_rows [Integer]
    #   Stop reading from IPC file after reading `n_rows`.
    # @param cache [Boolean]
    #   Cache the result after reading.
    # @param rechunk [Boolean]
    #   Reallocate to contiguous memory when all chunks/ files are parsed.
    # @param row_index_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_index_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param glob [Boolean]
    #   Expand path given via globbing rules.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param credential_provider [Object]
    #   Provide a function that can be called to provide cloud storage
    #   credentials. The function is expected to return a hash of
    #   credential keys along with an optional credential expiry time.
    # @param retries [Integer]
    #   Number of retries if accessing a cloud instance fails.
    # @param file_cache_ttl [Integer]
    #   Amount of time to keep downloaded cloud files since their last access time,
    #   in seconds. Uses the `POLARS_FILE_CACHE_TTL` environment variable
    #   (which defaults to 1 hour) if not given.
    # @param hive_partitioning [Boolean]
    #   Infer statistics and schema from Hive partitioned URL and use them
    #   to prune reads. This is unset by default (i.e. `nil`), meaning it is
    #   automatically enabled when a single directory is passed, and otherwise
    #   disabled.
    # @param hive_schema [Hash]
    #   The column names and data types of the columns by which the data is partitioned.
    #   If set to `nil` (default), the schema of the Hive partitions is inferred.
    # @param try_parse_hive_dates [Boolean]
    #   Whether to try parsing hive values as date/datetime types.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    #
    # @return [LazyFrame]
    def scan_ipc(
      source,
      n_rows: nil,
      cache: true,
      rechunk: false,
      row_index_name: nil,
      row_index_offset: 0,
      glob: true,
      storage_options: nil,
      credential_provider: "auto",
      retries: nil,
      file_cache_ttl: nil,
      hive_partitioning: nil,
      hive_schema: nil,
      try_parse_hive_dates: true,
      include_file_paths: nil,
      _record_batch_statistics: false
    )
      sources = get_sources(source)

      if !retries.nil?
        msg = "the `retries` parameter was deprecated in 0.25.0; specify 'max_retries' in `storage_options` instead."
        Utils.issue_deprecation_warning(msg)
        storage_options = storage_options || {}
        storage_options["max_retries"] = retries
      end

      if !file_cache_ttl.nil?
        msg = "the `file_cache_ttl` parameter was deprecated in 0.25.0; specify 'file_cache_ttl' in `storage_options` instead."
        Utils.issue_deprecation_warning(msg)
        storage_options = storage_options || {}
        storage_options["file_cache_ttl"] = file_cache_ttl
      end

      credential_provider_builder = _init_credential_provider_builder(
        credential_provider, sources, storage_options, "scan_parquet"
      )

      rblf =
        RbLazyFrame.new_from_ipc(
          sources,
          _record_batch_statistics,
          ScanOptions.new(
            row_index: !row_index_name.nil? ? [row_index_name, row_index_offset] : nil,
            pre_slice: !n_rows.nil? ? [0, n_rows] : nil,
            include_file_paths: include_file_paths,
            glob: glob,
            hive_partitioning: hive_partitioning,
            hive_schema: hive_schema,
            try_parse_hive_dates: try_parse_hive_dates,
            rechunk: rechunk,
            cache: cache,
            storage_options: !storage_options.nil? ? storage_options.to_a : nil,
            credential_provider: credential_provider_builder
          )
        )
      Utils.wrap_ldf(rblf)
    end
  end
end

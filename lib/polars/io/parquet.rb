module Polars
  module IO
    # Read into a DataFrame from a parquet file.
    #
    # @param source [String, Pathname, StringIO]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param parallel ["auto", "columns", "row_groups", "none"]
    #   This determines the direction of parallelism. 'auto' will try to determine the
    #   optimal direction.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    # @param use_statistics [Boolean]
    #   Use statistics in the parquet to determine if pages
    #   can be skipped from reading.
    # @param rechunk [Boolean]
    #   Make sure that all columns are contiguous in memory by
    #   aggregating the chunks into a single array.
    #
    # @return [DataFrame]
    #
    # @note
    #   This operation defaults to a `rechunk` operation at the end, meaning that
    #   all data will be stored continuously in memory.
    #   Set `rechunk: false` if you are benchmarking the parquet-reader. A `rechunk` is
    #   an expensive operation.
    def read_parquet(
      source,
      columns: nil,
      n_rows: nil,
      storage_options: nil,
      parallel: "auto",
      row_count_name: nil,
      row_count_offset: 0,
      low_memory: false,
      use_statistics: true,
      rechunk: true
    )
      _prepare_file_arg(source) do |data|
        DataFrame._read_parquet(
          data,
          columns: columns,
          n_rows: n_rows,
          parallel: parallel,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          low_memory: low_memory,
          use_statistics: use_statistics,
          rechunk: rechunk
        )
      end
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

      Plr.parquet_schema(source)
    end

    # Lazily read from a parquet file or multiple files via glob patterns.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a file.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param cache [Boolean]
    #   Cache the result after reading.
    # @param parallel ["auto", "columns", "row_groups", "none"]
    #   This determines the direction of parallelism. 'auto' will try to determine the
    #   optimal direction.
    # @param rechunk [Boolean]
    #   In case of reading multiple files via a glob pattern rechunk the final DataFrame
    #   into contiguous memory chunks.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    #
    # @return [LazyFrame]
    def scan_parquet(
      source,
      n_rows: nil,
      cache: true,
      parallel: "auto",
      glob: true,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      low_memory: false
    )
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      LazyFrame._scan_parquet(
        source,
        n_rows:n_rows,
        cache: cache,
        parallel: parallel,
        rechunk: rechunk,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
        storage_options: storage_options,
        low_memory: low_memory,
        glob: glob
      )
    end
  end
end

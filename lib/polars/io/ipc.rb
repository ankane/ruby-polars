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
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
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
      row_count_name: nil,
      row_count_offset: 0,
      rechunk: true
    )
      storage_options ||= {}
      _prepare_file_arg(source, **storage_options) do |data|
        DataFrame._read_ipc(
          data,
          columns: columns,
          n_rows: n_rows,
          row_count_name: row_count_name,
          row_count_offset: row_count_offset,
          rechunk: rechunk,
          memory_map: memory_map
        )
      end
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
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param storage_options [Hash]
    #   Extra options that make sense for a particular storage connection.
    # @param memory_map [Boolean]
    #   Try to memory map the file. This can greatly improve performance on repeated
    #   queries as the OS may cache pages.
    #   Only uncompressed IPC files can be memory mapped.
    #
    # @return [LazyFrame]
    def scan_ipc(
      source,
      n_rows: nil,
      cache: true,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0,
      storage_options: nil,
      memory_map: true
    )
      LazyFrame._scan_ipc(
        source,
        n_rows: n_rows,
        cache: cache,
        rechunk: rechunk,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
        storage_options: storage_options,
        memory_map: memory_map
      )
    end
  end
end

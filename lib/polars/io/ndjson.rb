module Polars
  module IO
    # Read into a DataFrame from a newline delimited JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [DataFrame]
    def read_ndjson(source)
      DataFrame._read_ndjson(source)
    end

    # Lazily read from a newline delimited JSON file.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a file.
    # @param infer_schema_length [Integer]
    #   Infer the schema length from the first `infer_schema_length` rows.
    # @param batch_size [Integer]
    #   Number of rows to read in each batch.
    # @param n_rows [Integer]
    #   Stop reading from JSON file after reading `n_rows`.
    # @param low_memory [Boolean]
    #   Reduce memory pressure at the expense of performance.
    # @param rechunk [Boolean]
    #   Reallocate to contiguous memory when all chunks/ files are parsed.
    # @param row_count_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_count_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    #
    # @return [LazyFrame]
    def scan_ndjson(
      source,
      infer_schema_length: 100,
      batch_size: 1024,
      n_rows: nil,
      low_memory: false,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0
    )
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end

      LazyFrame._scan_ndjson(
        source,
        infer_schema_length: infer_schema_length,
        batch_size: batch_size,
        n_rows: n_rows,
        low_memory: low_memory,
        rechunk: rechunk,
        row_count_name: row_count_name,
        row_count_offset: row_count_offset,
      )
    end
  end
end

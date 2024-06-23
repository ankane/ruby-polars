module Polars
  module IO
    # Read into a DataFrame from a newline delimited JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [DataFrame]
    def read_ndjson(
      source,
      schema: nil,
      schema_overrides: nil,
      ignore_errors: false
    )
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      rbdf =
        RbDataFrame.read_ndjson(
          source,
          ignore_errors,
          schema,
          schema_overrides
        )
      Utils.wrap_df(rbdf)
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
      infer_schema_length: N_INFER_DEFAULT,
      batch_size: 1024,
      n_rows: nil,
      low_memory: false,
      rechunk: true,
      row_count_name: nil,
      row_count_offset: 0
    )
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      rblf =
        RbLazyFrame.new_from_ndjson(
          source,
          infer_schema_length,
          batch_size,
          n_rows,
          low_memory,
          rechunk,
          Utils._prepare_row_count_args(row_count_name, row_count_offset)
        )
      Utils.wrap_ldf(rblf)
    end
  end
end

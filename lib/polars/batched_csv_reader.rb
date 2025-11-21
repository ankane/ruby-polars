module Polars
  # @private
  class BatchedCsvReader
    attr_accessor :_reader, :new_columns

    def initialize(
      source,
      has_header: true,
      columns: nil,
      separator: ",",
      comment_prefix: nil,
      quote_char: '"',
      skip_rows: 0,
      skip_lines: 0,
      schema_overrides: nil,
      null_values: nil,
      missing_utf8_is_empty_string: false,
      ignore_errors: false,
      try_parse_dates: false,
      n_threads: nil,
      infer_schema_length: 100,
      batch_size: 50_000,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_index_name: nil,
      row_index_offset: 0,
      eol_char: "\n",
      new_columns: nil,
      raise_if_empty: true,
      truncate_ragged_lines: false,
      decimal_comma: false
    )
      path = Utils.normalize_filepath(source)

      dtype_list = nil
      dtype_slice = nil
      if !schema_overrides.nil?
        if schema_overrides.is_a?(Hash)
          dtype_list = []
          schema_overrides.each do |k, v|
            dtype_list << [k, Utils.parse_into_dtype(v)]
          end
        elsif schema_overrides.is_a?(::Array)
          dtype_slice = schema_overrides
        else
          raise TypeError, "dtype arg should be array or hash"
        end
      end

      processed_null_values = Utils._process_null_values(null_values)
      projection, columns = Utils.handle_projection_columns(columns)

      self._reader = RbBatchedCsv.new(
        infer_schema_length,
        batch_size,
        has_header,
        ignore_errors,
        n_rows,
        skip_rows,
        skip_lines,
        projection,
        separator,
        rechunk,
        columns,
        encoding,
        n_threads,
        path,
        dtype_list,
        dtype_slice,
        low_memory,
        comment_prefix,
        quote_char,
        processed_null_values,
        missing_utf8_is_empty_string,
        try_parse_dates,
        skip_rows_after_header,
        Utils.parse_row_index_args(row_index_name, row_index_offset),
        eol_char,
        raise_if_empty,
        truncate_ragged_lines,
        decimal_comma
      )
      self.new_columns = new_columns
    end

    def next_batches(n)
      batches = _reader.next_batches(n)
      if !batches.nil?
        if new_columns
          batches.map { |df| Utils._update_columns(Utils.wrap_df(df), new_columns) }
        else
          batches.map { |df| Utils.wrap_df(df) }
        end
      else
        nil
      end
    end
  end
end

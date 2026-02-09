module Polars
  # @private
  class BatchedCsvReader
    attr_accessor :_reader

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
      q = Polars.scan_csv(
        source,
        infer_schema_length: infer_schema_length,
        has_header: has_header,
        ignore_errors: ignore_errors,
        n_rows: n_rows,
        skip_rows: skip_rows,
        skip_lines: skip_lines,
        separator: separator,
        rechunk: rechunk,
        encoding: encoding,
        schema_overrides: schema_overrides,
        low_memory: low_memory,
        comment_prefix: comment_prefix,
        quote_char: quote_char,
        null_values: null_values,
        missing_utf8_is_empty_string: missing_utf8_is_empty_string,
        try_parse_dates: try_parse_dates,
        skip_rows_after_header: skip_rows_after_header,
        row_index_name: row_index_name,
        row_index_offset: row_index_offset,
        eol_char: eol_char,
        raise_if_empty: raise_if_empty,
        truncate_ragged_lines: truncate_ragged_lines,
        decimal_comma: decimal_comma,
        new_columns: new_columns
      )

      if !columns.nil?
        q = q.select(columns)
      end

      # Trigger empty data.
      if raise_if_empty
        q.collect_schema
      end
      self._reader = q.collect_batches(chunk_size: batch_size)
    end

    def next_batches(n)
      chunks = self._reader.take(n)

      if chunks.length > 0
        return chunks
      end
      nil
    end
  end
end

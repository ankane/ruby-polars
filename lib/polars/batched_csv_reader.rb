module Polars
  # @private
  class BatchedCsvReader
    attr_accessor :_reader, :new_columns

    def initialize(
      file,
      has_header: true,
      columns: nil,
      sep: ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      parse_dates: false,
      n_threads: nil,
      infer_schema_length: 100,
      batch_size: 50_000,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      sample_size: 1024,
      eol_char: "\n",
      new_columns: nil
    )
      if Utils.pathlike?(file)
        path = Utils.normalise_filepath(file)
      end

      dtype_list = nil
      dtype_slice = nil
      if !dtypes.nil?
        if dtypes.is_a?(Hash)
          dtype_list = []
          dtypes.each do|k, v|
            dtype_list << [k, Utils.rb_type_to_dtype(v)]
          end
        elsif dtypes.is_a?(::Array)
          dtype_slice = dtypes
        else
          raise ArgumentError, "dtype arg should be list or dict"
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
        projection,
        sep,
        rechunk,
        columns,
        encoding,
        n_threads,
        path,
        dtype_list,
        dtype_slice,
        low_memory,
        comment_char,
        quote_char,
        processed_null_values,
        parse_dates,
        skip_rows_after_header,
        Utils._prepare_row_count_args(row_count_name, row_count_offset),
        sample_size,
        eol_char
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

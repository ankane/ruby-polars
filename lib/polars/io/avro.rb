module Polars
  module IO
    # Read into a DataFrame from Apache Avro format.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param columns [Object]
    #   Columns to select. Accepts a list of column indices (starting at zero) or a list
    #   of column names.
    # @param n_rows [Integer]
    #   Stop reading from Apache Avro file after reading ``n_rows``.
    #
    # @return [DataFrame]
    def read_avro(source, columns: nil, n_rows: nil)
      if Utils.pathlike?(source)
        source = Utils.normalise_filepath(source)
      end
      projection, column_names = Utils.handle_projection_columns(columns)

      rbdf = RbDataFrame.read_avro(source, column_names, projection, n_rows)
      Utils.wrap_df(rbdf)
    end
  end
end

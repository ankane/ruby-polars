module Polars
  module IO
    # Read into a DataFrame from a JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    # @param schema [Object]
    #   The DataFrame schema may be declared in several ways:
    #
    #   * As a dict of {name:type} pairs; if type is None, it will be auto-inferred.
    #   * As a list of column names; in this case types are automatically inferred.
    #   * As a list of (name,type) pairs; this is equivalent to the dictionary form.
    #
    #   If you supply a list of column names that does not match the names in the
    #   underlying data, the names given here will overwrite them. The number
    #   of names given in the schema should match the underlying data dimensions.
    # @param schema_overrides [Hash]
    #   Support type specification or override of one or more columns; note that
    #   any dtypes inferred from the schema param will be overridden.
    # @param infer_schema_length [Integer]
    #   The maximum number of rows to scan for schema inference.
    #   If set to `nil`, the full data may be scanned *(this is slow)*.
    #
    # @return [DataFrame]
    def read_json(
      source,
      schema: nil,
      schema_overrides: nil,
      infer_schema_length: N_INFER_DEFAULT
    )
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      rbdf =
        RbDataFrame.read_json(
          source,
          infer_schema_length,
          schema,
          schema_overrides
        )
      Utils.wrap_df(rbdf)
    end
  end
end

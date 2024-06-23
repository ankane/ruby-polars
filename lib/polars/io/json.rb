module Polars
  module IO
    # Read into a DataFrame from a JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
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

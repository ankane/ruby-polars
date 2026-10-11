module Polars
  module IO
    # Scan an external reader.
    #
    # @param reader_builder [Object]
    #   The external reader builder.
    # @param sources [Object]
    #   The sources to scan.
    # @param schema [Object]
    #   The schema of the data.
    # @param expand_paths [Boolean]
    #   Whether to expand provided paths.
    #
    # @return [LazyFrame]
    def scan_external_reader(
      reader_builder,
      sources:,
      schema:,
      expand_paths: false
    )
      Utils.wrap_ldf(
        RbLazyFrame.new_from_external_reader_builder(
          sources,
          reader_builder,
          schema,
          ScanOptions(expand_paths: expand_paths)
        )
      )
    end
  end
end

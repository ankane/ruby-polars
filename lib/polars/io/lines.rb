module Polars
  module IO
    # Read lines into a string column from a file.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param source [Object]
    #   Path(s) to a file or directory
    #   When needing to authenticate for scanning cloud locations, see the
    #   `storage_options` parameter.
    # @param name [String]
    #   Name to use for the output column.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param row_index_name [String]
    #   If not nil, this will insert a row index column with the given name into the
    #   DataFrame
    # @param row_index_offset [Integer]
    #   Offset to start the row index column (only used if the name is set)
    # @param glob [Boolean]
    #   Expand path given via globbing rules.
    # @param storage_options [Hash]
    #   Options that indicate how to connect to a cloud provider.
    #
    #   The cloud providers currently supported are AWS, GCP, and Azure.
    #   See supported keys here:
    #
    #   * [aws](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    #   * [gcp](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    #   * [azure](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    #   * Hugging Face (`hf://`): Accepts an API key under the `token` parameter: \
    #     `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.
    #
    #   If `storage_options` is not provided, Polars will try to infer the information
    #   from environment variables.
    # @param credential_provider [Object]
    #   Provide a function that can be called to provide cloud storage
    #   credentials. The function is expected to return a dictionary of
    #   credential keys along with an optional credential expiry time.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    #
    # @return [DataFrame]
    #
    # @example
    #   Polars.read_lines(StringIO.new("Hello\nworld"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────┐
    #   # │ lines │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │ Hello │
    #   # │ world │
    #   # └───────┘
    def read_lines(
      source,
      name: "lines",
      n_rows: nil,
      row_index_name: nil,
      row_index_offset: 0,
      glob: true,
      storage_options: nil,
      credential_provider: "auto",
      include_file_paths: nil
    )
      scan_lines(
        source,
        name: name,
        n_rows: n_rows,
        row_index_name: row_index_name,
        row_index_offset: row_index_offset,
        glob: glob,
        storage_options: storage_options,
        credential_provider: credential_provider,
        include_file_paths: include_file_paths
      ).collect
    end

    # Construct a LazyFrame which scans lines into a string column from a file.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param source [Object]
    #   Path(s) to a file or directory
    #   When needing to authenticate for scanning cloud locations, see the
    #   `storage_options` parameter.
    # @param name [String]
    #   Name to use for the output column.
    # @param n_rows [Integer]
    #   Stop reading from parquet file after reading `n_rows`.
    # @param row_index_name [String]
    #   If not nil, this will insert a row index column with the given name into the
    #   DataFrame
    # @param row_index_offset
    #   Offset to start the row index column (only used if the name is set)
    # @param glob [Boolean]
    #   Expand path given via globbing rules.
    # @param storage_options [Hash]
    #   Options that indicate how to connect to a cloud provider.
    #
    #   The cloud providers currently supported are AWS, GCP, and Azure.
    #   See supported keys here:
    #
    #   * [aws](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    #   * [gcp](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    #   * [azure](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    #   * Hugging Face (`hf://`): Accepts an API key under the `token` parameter: \
    #     `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.
    #
    #   If `storage_options` is not provided, Polars will try to infer the information
    #   from environment variables.
    # @param credential_provider [Object]
    #   Provide a function that can be called to provide cloud storage
    #   credentials. The function is expected to return a dictionary of
    #   credential keys along with an optional credential expiry time.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    #
    # @return [LazyFrame]
    #
    # @example
    #   Polars.scan_lines(StringIO.new("Hello\nworld")).collect
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────┐
    #   # │ lines │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │ Hello │
    #   # │ world │
    #   # └───────┘
    def scan_lines(
      source,
      name: "lines",
      n_rows: nil,
      row_index_name: nil,
      row_index_offset: 0,
      glob: true,
      storage_options: nil,
      credential_provider: "auto",
      include_file_paths: nil
    )
      sources = get_sources(source)

      credential_provider_builder = _init_credential_provider_builder(
        credential_provider, sources, storage_options, "scan_lines"
      )

      rblf = RbLazyFrame.new_from_scan_lines(
        sources,
        ScanOptions.new(
          row_index: !row_index_name.nil? ? [row_index_name, row_index_offset] : nil,
          pre_slice: !n_rows.nil? ? [0, n_rows] : nil,
          include_file_paths: include_file_paths,
          glob: glob,
          storage_options: storage_options,
          credential_provider: credential_provider_builder
        ),
        name
      )

      Utils.wrap_ldf(rblf)
    end
  end
end

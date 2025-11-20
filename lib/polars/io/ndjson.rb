module Polars
  module IO
    # Read into a DataFrame from a newline delimited JSON file.
    #
    # @param source [String]
    #   Path to a file.
    # @param schema [Object]
    #   The DataFrame schema may be declared in several ways:
    #
    #   * As a dict of \\\\{name:type} pairs; if type is nil, it will be auto-inferred.
    #   * As a list of column names; in this case types are automatically inferred.
    #   * As a list of (name,type) pairs; this is equivalent to the hash form.
    #
    #   If you supply a list of column names that does not match the names in the
    #   underlying data, the names given here will overwrite them. The number
    #   of names given in the schema should match the underlying data dimensions.
    # @param schema_overrides [Hash]
    #   Support type specification or override of one or more columns; note that
    #   any dtypes inferred from the schema param will be overridden.
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
    # @param row_index_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_index_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param ignore_errors [Boolean]
    #   Return `Null` if parsing fails because of schema mismatches.
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
    #   credentials. The function is expected to return a hash of
    #   credential keys along with an optional credential expiry time.
    # @param retries [Integer]
    #   Number of retries if accessing a cloud instance fails.
    # @param file_cache_ttl [Integer]
    #   Amount of time to keep downloaded cloud files since their last access time,
    #   in seconds. Uses the `POLARS_FILE_CACHE_TTL` environment variable
    #   (which defaults to 1 hour) if not given.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    #
    # @return [DataFrame]
    def read_ndjson(
      source,
      schema: nil,
      schema_overrides: nil,
      infer_schema_length: N_INFER_DEFAULT,
      batch_size: 1024,
      n_rows: nil,
      low_memory: false,
      rechunk: false,
      row_index_name: nil,
      row_index_offset: 0,
      ignore_errors: false,
      storage_options: nil,
      credential_provider: "auto",
      retries: 2,
      file_cache_ttl: nil,
      include_file_paths: nil
    )
      credential_provider_builder = _init_credential_provider_builder(
        credential_provider, source, storage_options, "read_ndjson"
      )

      scan_ndjson(
        source,
        schema: schema,
        schema_overrides: schema_overrides,
        infer_schema_length: infer_schema_length,
        batch_size: batch_size,
        n_rows: n_rows,
        low_memory: low_memory,
        rechunk: rechunk,
        row_index_name: row_index_name,
        row_index_offset: row_index_offset,
        ignore_errors: ignore_errors,
        include_file_paths: include_file_paths,
        retries: retries,
        storage_options: storage_options,
        credential_provider: credential_provider_builder,
        file_cache_ttl: file_cache_ttl,
      ).collect
    end

    # Lazily read from a newline delimited JSON file.
    #
    # This allows the query optimizer to push down predicates and projections to the scan
    # level, thereby potentially reducing memory overhead.
    #
    # @param source [String]
    #   Path to a file.
    # @param schema [Object]
    #   The DataFrame schema may be declared in several ways:
    #
    #   * As a dict of \\\\{name:type} pairs; if type is nil, it will be auto-inferred.
    #   * As a list of column names; in this case types are automatically inferred.
    #   * As a list of (name,type) pairs; this is equivalent to the hash form.
    #
    #   If you supply a list of column names that does not match the names in the
    #   underlying data, the names given here will overwrite them. The number
    #   of names given in the schema should match the underlying data dimensions.
    # @param schema_overrides [Hash]
    #   Support type specification or override of one or more columns; note that
    #   any dtypes inferred from the schema param will be overridden.
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
    # @param row_index_name [String]
    #   If not nil, this will insert a row count column with give name into the
    #   DataFrame.
    # @param row_index_offset [Integer]
    #   Offset to start the row_count column (only use if the name is set).
    # @param ignore_errors [Boolean]
    #   Return `Null` if parsing fails because of schema mismatches.
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
    #   credentials. The function is expected to return a hash of
    #   credential keys along with an optional credential expiry time.
    # @param retries [Integer]
    #   Number of retries if accessing a cloud instance fails.
    # @param file_cache_ttl [Integer]
    #   Amount of time to keep downloaded cloud files since their last access time,
    #   in seconds. Uses the `POLARS_FILE_CACHE_TTL` environment variable
    #   (which defaults to 1 hour) if not given.
    # @param include_file_paths [String]
    #   Include the path of the source file(s) as a column with this name.
    #
    # @return [LazyFrame]
    def scan_ndjson(
      source,
      schema: nil,
      schema_overrides: nil,
      infer_schema_length: N_INFER_DEFAULT,
      batch_size: 1024,
      n_rows: nil,
      low_memory: false,
      rechunk: false,
      row_index_name: nil,
      row_index_offset: 0,
      ignore_errors: false,
      storage_options: nil,
      credential_provider: "auto",
      retries: 2,
      file_cache_ttl: nil,
      include_file_paths: nil
    )
      sources = []
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      elsif source.is_a?(::Array)
        if Utils.is_path_or_str_sequence(source)
          sources = source.map { |s| Utils.normalize_filepath(s) }
        else
          sources = source
        end

        source = nil
      end

      if infer_schema_length == 0
        msg = "'infer_schema_length' should be positive"
        raise ArgumentError, msg
      end

      credential_provider_builder = _init_credential_provider_builder(
        credential_provider, source, storage_options, "scan_ndjson"
      )

      if storage_options&.any?
        storage_options = storage_options.map { |k, v| [k.to_s, v.to_s] }
      else
        storage_options = nil
      end

      rblf =
        RbLazyFrame.new_from_ndjson(
          source,
          sources,
          infer_schema_length,
          schema,
          schema_overrides,
          batch_size,
          n_rows,
          low_memory,
          rechunk,
          Utils.parse_row_index_args(row_index_name, row_index_offset),
          ignore_errors,
          include_file_paths,
          storage_options,
          credential_provider_builder,
          retries,
          file_cache_ttl
        )
      Utils.wrap_ldf(rblf)
    end
  end
end

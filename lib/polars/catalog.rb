module Polars
  # Unity catalog client.
  class Catalog
    # Initialize a catalog client.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param workspace_url [String]
    #   URL of the workspace, or alternatively the URL of the Unity catalog
    #   API endpoint.
    # @param bearer_token [String]
    #   Bearer token to authenticate with. This can also be set to:
    #   * "auto": Automatically retrieve bearer tokens from the environment.
    #   * "databricks-sdk": Use the Databricks SDK to retrieve and use the
    #     bearer token from the environment.
    # @param require_https [Boolean]
    #   Require the `workspace_url` to use HTTPS.
    def initialize(workspace_url, bearer_token: "auto", require_https: true)
      if require_https && !workspace_url.start_with?("https://")
        msg = (
          "a non-HTTPS workspace_url was given. To " +
          "allow non-HTTPS URLs, pass require_https: false."
        )
        raise ArgumentError, msg
      end

      if bearer_token == "auto"
        bearer_token = nil
      end

      @client = RbCatalogClient.new(workspace_url, bearer_token)
    end

    # List the available catalogs.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @return [Array]
    def list_catalogs
      @client.list_catalogs
    end

    # List the available namespaces (unity schema) under the specified catalog.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param catalog_name [String]
    #   Name of the catalog.
    #
    # @return [Array]
    def list_namespaces(catalog_name)
      @client.list_namespaces(catalog_name)
    end

    # List the available tables under the specified schema.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param catalog_name [String]
    #   Name of the catalog.
    # @param namespace [String]
    #   Name of the namespace (unity schema).
    #
    # @return [Array]
    def list_tables(catalog_name, namespace)
      @client.list_tables(catalog_name, namespace)
    end

    # Retrieve the metadata of the specified table.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param catalog_name [String]
    #   Name of the catalog.
    # @param namespace [String]
    #   Name of the namespace (unity schema).
    # @param table_name [String]
    #   Name of the table.
    #
    # @return [TableInfo]
    def get_table_info(catalog_name, namespace, table_name)
      @client.get_table_info(catalog_name, namespace, table_name)
    end

    # Retrieve the metadata of the specified table.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param catalog_name [String]
    #   Name of the catalog.
    # @param namespace [String]
    #   Name of the namespace (unity schema).
    # @param table_name [String]
    #   Name of the table.
    # @param delta_table_version [Object]
    #   Version of the table to scan (Deltalake only).
    # @param delta_table_options [Hash]
    #   Additional keyword arguments while reading a Deltalake table.
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
    #   `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.
    #
    #   If `storage_options` is not provided, Polars will try to infer the
    #   information from environment variables.
    #
    # @return [LazyFrame]
    def scan_table(
      catalog_name,
      namespace,
      table_name,
      delta_table_version: nil,
      delta_table_options: nil,
      storage_options: nil
    )
      table_info = get_table_info(catalog_name, namespace, table_name)
      storage_location, data_source_format = _extract_location_and_data_format(
        table_info, "scan table"
      )

      if ["DELTA", "DELTASHARING"].include?(data_source_format)
        return Polars.scan_delta(
          storage_location,
          version: delta_table_version,
          delta_table_options: delta_table_options,
          storage_options: storage_options
        )
      end

      if !delta_table_version.nil?
        msg = (
          "cannot apply delta_table_version for table of type " +
          "#{data_source_format}"
        )
        raise ArgumentError, msg
      end

      if !delta_table_options.nil?
        msg = (
          "cannot apply delta_table_options for table of type " +
          "#{data_source_format}"
        )
        raise ArgumentError, msg
      end

      if storage_options&.any?
        storage_options = storage_options.to_a
      else
        # Handle empty dict input
        storage_options = nil
      end

      raise Todo
    end

    private

    def _extract_location_and_data_format(table_info, operation)
      if table_info.storage_location.nil?
        msg = "cannot #{operation}: no storage_location found"
        raise ArgumentError, msg
      end

      if table_info.data_source_format.nil?
        msg = "cannot #{operation}: no data_source_format found"
        raise ArgumentError, msg
      end

      [table_info.storage_location, table_info.data_source_format]
    end
  end
end

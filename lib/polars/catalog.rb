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
  end
end

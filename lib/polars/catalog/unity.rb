# frozen_string_literal: true

require_relative "api_client"
require_relative "client"
require_relative "error"
require_relative "models"

module Polars
  module Catalog
    # A client for the Databricks Unity Catalog.
    class Unity
      # @param workspace_url [String] The URL of the Databricks workspace.
      # @param bearer_token [String] The personal access token for authentication.
      # @param require_https [Boolean] Whether to enforce HTTPS for the workspace URL.
      #
      # @return [Polars::Catalog::Unity::Client]
      def self.new(workspace_url, bearer_token:, require_https: true)
        if require_https && !workspace_url.start_with?("https://")
          raise ArgumentError, "a non-HTTPS workspace_url was given (#{workspace_url}). To allow non-HTTPS URLs, pass require_https: false."
        end
        api_client = ApiClient.new(workspace_url, bearer_token)
        Client.new(api_client)
      end
    end
  end
end

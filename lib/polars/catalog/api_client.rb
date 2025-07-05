# frozen_string_literal: true

require 'net/http'
require 'json'
require 'uri'

module Polars
  module Catalog
    class Unity
      class ApiClient
        def initialize(workspace_url, bearer_token)
          @base_uri = URI.parse(workspace_url)
          @bearer_token = bearer_token
        end

        def get(path, params = {})
          uri = @base_uri.dup
          uri.path = path
          uri.query = URI.encode_www_form(params) unless params.empty?

          request = Net::HTTP::Get.new(uri)
          perform_request(request)
        end

        def post(path, body)
          uri = @base_uri.dup
          uri.path = path

          request = Net::HTTP::Post.new(uri)
          request.body = body.to_json
          perform_request(request)
        end

        private

        def perform_request(request)
          if @base_uri.host.nil?
            raise ArgumentError, "Base URI is not set or invalid: #{@base_uri}"
          end

          request['Authorization'] = "Bearer #{@bearer_token}"
          request['Content-Type'] = 'application/json'

          http = Net::HTTP.new(@base_uri.host, @base_uri.port)
          http.use_ssl = (@base_uri.scheme == 'https')

          response = http.request(request)

          if response.is_a?(Net::HTTPSuccess)
            response.body.empty? ? {} : JSON.parse(response.body)
          else
            raise Error, "API request failed: #{response.code} #{response.message} #{response.body}"
          end
        end
      end
    end
  end
end

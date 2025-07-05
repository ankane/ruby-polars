# test/catalog/unity_test.rb

require "test_helper"
require "webmock/minitest"

class UnityCatalogTest < Minitest::Test
  def setup
    @workspace_url = "https://test.workspace.com"
    @token = "test_token"
    WebMock.disable_net_connect!
  end

  def teardown
    WebMock.reset!
    WebMock.allow_net_connect!
  end

  # Polars::Catalog::Unity tests
  def test_unity_new_enforces_https_by_default
    assert_raises(ArgumentError) do
      Polars::Catalog::Unity.new("http://insecure.workspace.com", bearer_token: @token)
    end
  end

  def test_unity_new_allows_http_when_specified
    # Should not raise
    Polars::Catalog::Unity.new("http://insecure.workspace.com", bearer_token: @token, require_https: false)
  end

  def test_unity_new_returns_client
    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    assert_instance_of Polars::Catalog::Unity::Client, client
  end

  # Polars::Catalog::Unity::Client tests
  def test_list_catalogs
    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/catalogs")
      .with(headers: { "Authorization" => "Bearer #{@token}" })
      .to_return(status: 200, body: { catalogs: [{ name: "main" }] }.to_json, headers: { "Content-Type" => "application/json" })

    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    catalogs = client.list_catalogs

    assert_equal 1, catalogs.length
    assert_instance_of Polars::Catalog::Unity::Models::CatalogInfo, catalogs.first
    assert_equal "main", catalogs.first.name
  end

  def test_list_namespaces
    catalog_name = "main"
    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/schemas?catalog_name=#{catalog_name}")
      .with(headers: { "Authorization" => "Bearer #{@token}" })
      .to_return(status: 200, body: { schemas: [{ name: "default" }] }.to_json, headers: { "Content-Type" => "application/json" })

    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    namespaces = client.list_namespaces(catalog_name)

    assert_equal 1, namespaces.length
    assert_instance_of Polars::Catalog::Unity::Models::NamespaceInfo, namespaces.first
    assert_equal "default", namespaces.first.name
  end

  def test_list_tables
    catalog_name = "main"
    namespace_name = "default"
    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/tables?catalog_name=#{catalog_name}&schema_name=#{namespace_name}")
      .with(headers: { "Authorization" => "Bearer #{@token}" })
      .to_return(status: 200, body: { tables: [{ name: "my_table", table_type: "MANAGED", data_source_format: "DELTA" }] }.to_json, headers: { "Content-Type" => "application/json" })

    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    tables = client.list_tables(catalog_name, namespace_name)

    assert_equal 1, tables.length
    assert_instance_of Polars::Catalog::Unity::Models::TableInfo, tables.first
    assert_equal "my_table", tables.first.name
  end

  def test_get_table_info
    full_name = "main.default.my_table"
    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/tables/#{full_name}")
      .with(headers: { "Authorization" => "Bearer #{@token}" })
      .to_return(status: 200, body: { name: "my_table", storage_location: "s3://bucket/path", data_source_format: "DELTA", columns: [] }.to_json, headers: { "Content-Type" => "application/json" })

    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    table_info = client.get_table_info("main", "default", "my_table")

    assert_instance_of Polars::Catalog::Unity::Models::TableInfo, table_info
    assert_equal "my_table", table_info.name
  end

  def test_scan_delta_table
    table_info_response = {
      name: "delta_table",
      storage_location: "s3://bucket/delta",
      data_source_format: "DELTA",
      columns: []
    }.to_json

    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/tables/main.default.delta_table")
      .to_return(status: 200, body: table_info_response, headers: { "Content-Type" => "application/json" })

    Polars.stub(:scan_delta, ->(path, **_options) { "scanned_#{path}" }) do
      client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
      result = client.scan_table("main", "default", "delta_table")
      assert_equal "scanned_s3://bucket/delta", result
    end
  end

  def test_scan_parquet_table
    table_info_response = {
      name: "parquet_table",
      storage_location: "s3://bucket/parquet",
      data_source_format: "PARQUET",
      columns: []
    }.to_json

    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/tables/main.default.parquet_table")
      .to_return(status: 200, body: table_info_response, headers: { "Content-Type" => "application/json" })

    Polars.stub(:scan_parquet, ->(path, **_options) { "scanned_#{path}" }) do
      client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
      result = client.scan_table("main", "default", "parquet_table")
      assert_equal "scanned_s3://bucket/parquet", result
    end
  end

  def test_scan_table_raises_for_unsupported_format
    table_info_response = {
      name: "avro_table",
      storage_location: "s3://bucket/avro",
      data_source_format: "AVRO",
      columns: []
    }.to_json

    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/tables/main.default.avro_table")
      .to_return(status: 200, body: table_info_response, headers: { "Content-Type" => "application/json" })

    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    assert_raises(NotImplementedError) do
      client.scan_table("main", "default", "avro_table")
    end
  end

  # Polars::Catalog::Unity::ApiClient tests
  def test_api_client_raises_on_http_error
    stub_request(:get, "#{@workspace_url}/api/2.1/unity-catalog/catalogs")
      .to_return(status: [401, "Unauthorized"], headers: { "Content-Type" => "application/json" })

    client = Polars::Catalog::Unity.new(@workspace_url, bearer_token: @token)
    err = assert_raises(Polars::Catalog::Unity::Error) do
      client.list_catalogs
    end
    assert_match(/API request failed: 401 Unauthorized/, err.message)
  end
end
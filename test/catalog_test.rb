require_relative "test_helper"

class CatalogTest < Minitest::Test
  def setup
    skip unless ENV["TEST_CATALOG"]
    super
  end

  def test_list_catalogs
    catalogs = self.catalog.list_catalogs
    catalog = catalogs.find { |c| c.name == "unity" }
    assert_equal "unity", catalog.name
    assert_equal "Main catalog", catalog.comment
    assert_equal ({}), catalog.properties
    assert_equal ({}), catalog.options
    assert_nil catalog.storage_location
    assert_kind_of ::Time, catalog.created_at
    assert_nil catalog.created_by
    assert_nil catalog.updated_at
    assert_nil catalog.updated_by
  end

  def test_list_namespaces
    namespaces = catalog.list_namespaces("unity")
    namespace = namespaces.find { |n| n.name == "default" }
    assert_equal "default", namespace.name
    assert_equal "Default schema", namespace.comment
    assert_equal ({}), namespace.properties
    assert_nil namespace.storage_location
    assert_kind_of ::Time, namespace.created_at
    assert_nil namespace.created_by
    assert_nil namespace.updated_at
    assert_nil namespace.updated_by
  end

  def test_list_tables
    tables = catalog.list_tables("unity", "default")
    table = tables.find { |t| t.name == "marksheet" }
    assert_equal "marksheet", table.name
    assert_equal "Managed table", table.comment
    assert_equal "MANAGED", table.table_type
    assert_equal "DELTA", table.data_source_format
    assert_equal 3, table.columns.size

    column = table.columns[0]
    assert_equal "id", column.name
    assert_equal "INT", column.type_name
    assert_equal "int", column.type_text
    assert_equal %!{"name":"id","type":"integer","nullable":false,"metadata":{}}!, column.type_json
    assert_equal 0, column.position
    assert_equal "ID primary key", column.comment
    assert_nil column.partition_index
  end

  def test_get_table_info
    table = catalog.get_table_info("unity", "default", "marksheet")
    assert_equal "marksheet", table.name
    assert_equal "Managed table", table.comment
    assert_equal "MANAGED", table.table_type
    assert_equal "DELTA", table.data_source_format
    assert_equal 3, table.columns.size

    column = table.columns[0]
    assert_equal "id", column.name
    assert_equal "INT", column.type_name
    assert_equal "int", column.type_text
    assert_equal %!{"name":"id","type":"integer","nullable":false,"metadata":{}}!, column.type_json
    assert_equal 0, column.position
    assert_equal "ID primary key", column.comment
    assert_nil column.partition_index
  end

  def test_scan_table
    skip unless ENV["TEST_DELTA"]

    df = catalog.scan_table("unity", "default", "marksheet").collect
    assert_equal [15, 3], df.shape
  end

  def test_create_catalog
    self.catalog.delete_catalog("polars_ruby_test", force: true)
    catalog = self.catalog.create_catalog("polars_ruby_test")
    assert_equal "polars_ruby_test", catalog.name
    assert_includes self.catalog.list_catalogs.map(&:name), "polars_ruby_test"
  end

  def test_create_namespace
    catalog.delete_catalog("polars_ruby_test", force: true)
    catalog.create_catalog("polars_ruby_test")
    namespace = catalog.create_namespace("polars_ruby_test", "test_namespace")
    assert_equal "test_namespace", namespace.name
    assert_includes catalog.list_namespaces("polars_ruby_test").map(&:name), "test_namespace"
  end

  def test_get_polars_schema
    table = catalog.get_table_info("unity", "default", "marksheet")
    schema = table.get_polars_schema
    assert_kind_of Polars::Schema, schema
    assert_equal schema["id"], Polars::Int32
    assert_equal schema["name"], Polars::String
    assert_equal schema["marks"], Polars::Int32
  end

  def test_require_https
    error = assert_raises(ArgumentError) do
      Polars::Catalog.new("http://localhost:8080")
    end
    assert_equal "a non-HTTPS workspace_url was given. To allow non-HTTPS URLs, pass require_https: false.", error.message
  end

  def catalog
    @catalog ||= Polars::Catalog.new("http://localhost:8080", require_https: false)
  end
end

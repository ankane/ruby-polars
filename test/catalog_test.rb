require_relative "test_helper"

class CatalogTest < Minitest::Test
  def setup
    skip unless ENV["TEST_CATALOG"]
    super
  end

  def test_list_catalogs
    catalogs = catalog.list_catalogs
    assert_equal 1, catalogs.size

    assert_equal "unity", catalogs[0].name
    assert_equal "Main catalog", catalogs[0].comment
    assert_equal ({}), catalogs[0].properties
    assert_equal ({}), catalogs[0].options
    assert_nil catalogs[0].storage_location
    assert_kind_of ::Time, catalogs[0].created_at
    assert_nil catalogs[0].created_by
    assert_nil catalogs[0].updated_at
    assert_nil catalogs[0].updated_by
  end

  def test_list_namespaces
    namespaces = catalog.list_namespaces("unity")
    assert_equal 1, namespaces.size

    namespace = namespaces[0]
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
    assert_equal 4, tables.size

    table = tables[0]
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

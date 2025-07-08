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

  def test_write_table
    skip unless ENV["TEST_DELTA"]

    reset_catalog
    catalog.create_catalog("polars_ruby_test")
    catalog.create_namespace("polars_ruby_test", "test_namespace")
    catalog.create_table(
      "polars_ruby_test",
      "test_namespace",
      "test_table",
      schema: {},
      table_type: "EXTERNAL",
      data_source_format: "DELTA",
      storage_root: temp_path
    )

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    catalog.write_table(df, "polars_ruby_test", "test_namespace", "test_table")

    df2 = catalog.scan_table("polars_ruby_test", "test_namespace", "test_table").collect
    assert_frame df, df2
  end

  def test_create_catalog
    reset_catalog

    catalog = self.catalog.create_catalog("polars_ruby_test")
    assert_equal "polars_ruby_test", catalog.name
    assert_includes self.catalog.list_catalogs.map(&:name), "polars_ruby_test"

    self.catalog.delete_catalog("polars_ruby_test")
    refute_includes self.catalog.list_catalogs.map(&:name), "polars_ruby_test"
  end

  def test_create_namespace
    reset_catalog
    catalog.create_catalog("polars_ruby_test")

    namespace = catalog.create_namespace("polars_ruby_test", "test_namespace")
    assert_equal "test_namespace", namespace.name
    assert_includes catalog.list_namespaces("polars_ruby_test").map(&:name), "test_namespace"

    catalog.delete_namespace("polars_ruby_test", "test_namespace")
    refute_includes catalog.list_namespaces("polars_ruby_test").map(&:name), "test_namespace"
  end

  def test_create_table
    reset_catalog
    catalog.create_catalog("polars_ruby_test")
    catalog.create_namespace("polars_ruby_test", "test_namespace")

    table =
      catalog.create_table(
        "polars_ruby_test",
        "test_namespace",
        "test_table",
        schema: {},
        table_type: "EXTERNAL",
        data_source_format: "DELTA",
        storage_root: temp_path
      )
    assert_equal "test_table", table.name
    assert_includes catalog.list_tables("polars_ruby_test", "test_namespace").map(&:name), "test_table"

    catalog.delete_table("polars_ruby_test", "test_namespace", "test_table")
    refute_includes catalog.list_tables("polars_ruby_test", "test_namespace").map(&:name), "test_table"
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

  def reset_catalog
    catalog.delete_catalog("polars_ruby_test", force: true)
  rescue Polars::ComputeError
    # do nothing
  end
end

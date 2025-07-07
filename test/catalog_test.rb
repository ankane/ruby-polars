require_relative "test_helper"

class CatalogTest < Minitest::Test
  def setup
    skip unless ENV["TEST_CATALOG"]
    super
  end

  def test_list_catalogs
    catalog = Polars::Catalog.new("http://localhost:8080", require_https: false)
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
    catalog = Polars::Catalog.new("http://localhost:8080", require_https: false)
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

  def test_require_https
    error = assert_raises(ArgumentError) do
      Polars::Catalog.new("http://localhost:8080")
    end
    assert_equal "a non-HTTPS workspace_url was given. To allow non-HTTPS URLs, pass require_https: false.", error.message
  end
end

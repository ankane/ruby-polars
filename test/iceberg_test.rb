require_relative "test_helper"

class IcebergTest < Minitest::Test
  def setup
    skip unless ENV["TEST_ICEBERG"]
    super
    catalog.create_namespace("polars_ruby_test", if_not_exists: true)
    catalog.drop_table("polars_ruby_test.events", if_exists: true)
  end

  def test_write_iceberg_mode_append
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [true, false, true]})
    df2 = Polars::DataFrame.new({"a" => [4, 5, 6], "b" => [false, false, true]})
    table = catalog.create_table("polars_ruby_test.events", schema: df.schema)
    assert_nil df.write_iceberg(table, mode: "append")
    assert_nil df2.write_iceberg(table, mode: "append")
    assert_equal Polars.concat([df, df2]), Polars.scan_iceberg(table).collect
  end

  private

  def catalog
    @catalog ||= Iceberg::RestCatalog.new(uri: "http://localhost:8181")
  end
end

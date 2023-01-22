require_relative "test_helper"

class ParquetTest < Minitest::Test
  def test_read_parquet
    df = Polars.read_parquet("test/support/data.parquet")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_parquet_file
    df = File.open("test/support/data.parquet", "rb") { |f| Polars.read_parquet(f) }
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_scan_parquet
    df = Polars.scan_parquet("test/support/data.parquet")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_read_parquet_schema
    schema = Polars.read_parquet_schema("test/support/data.parquet")
    assert_equal ({"a" => Polars::Int64, "b" => Polars::Utf8}), schema
  end

  def test_write_parquet
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_nil df.write_parquet(temp_path)
  end

  def test_write_parquet_struct
    df = Polars::DataFrame.new({"a" => [{"f1" => 1}, {"f1" => 2}]})
    assert_nil df.write_parquet(temp_path)
  end

  def test_write_parquet_struct_nested
    data = [
      {"a" => {"b" => {"c" => 1}}},
      {"a" => {"b" => {"c" => 2}}}
    ]
    df = Polars::DataFrame.new({"s" => data})
    assert_nil df.write_parquet(temp_path)
  end

  def test_types
    df = Polars.read_parquet("test/support/types.parquet")
    assert_nil df.write_parquet(temp_path)
  end
end

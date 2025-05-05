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

  def test_read_parquet_pathname
    require "pathname"

    df = Polars.read_parquet(Pathname.new("test/support/data.parquet"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_parquet_io
    io = StringIO.new(File.binread("test/support/data.parquet"))
    df = Polars.read_parquet(io)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_parquet_glob
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, Polars.read_parquet("test/support/data*.parquet")
  end

  def test_read_parquet_array
    df = Polars.read_parquet(["test/support/data.parquet"] * 2)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3, 1, 2, 3], "b" => ["one", "two", "three", "one", "two", "three"]})
    assert_frame expected, df
  end

  def test_scan_parquet
    df = Polars.scan_parquet("test/support/data.parquet")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_scan_parquet_array
    df = Polars.scan_parquet(["test/support/data.parquet"] * 2)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3, 1, 2, 3], "b" => ["one", "two", "three", "one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_scan_parquet_io
    io = StringIO.new(File.binread("test/support/data.parquet"))
    df = Polars.scan_parquet(io)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_scan_parquet_cloud
    skip unless cloud?

    df = Polars.scan_parquet(cloud_file("data.parquet"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_read_parquet_schema
    schema = Polars.read_parquet_schema("test/support/data.parquet")
    assert_equal ({"a" => Polars::Int64, "b" => Polars::String}), schema
  end

  def test_write_parquet
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_nil df.write_parquet(temp_path)
  end

  def test_write_parquet_pathname
    require "pathname"

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_nil df.write_parquet(Pathname.new(temp_path))
  end

  def test_write_parquet_io
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    io = StringIO.new
    df.write_parquet(io)
    io.rewind
    assert_frame df, Polars.read_parquet(io)
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

  def test_sink_parquet
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.lazy.sink_parquet(path)
    assert_frame df, Polars.read_parquet(path)
  end

  def test_sink_parquet_io
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    io = StringIO.new
    df.lazy.sink_parquet(io)
    io.rewind
    assert_frame df, Polars.read_parquet(io)
  end

  def test_types
    df = Polars.read_parquet("test/support/types.parquet")
    assert_nil df.write_parquet(temp_path)
  end

  def test_decimals
    Polars::Config.activate_decimals
    df = Polars::DataFrame.new({"a" => [BigDecimal("1"), BigDecimal("2"), BigDecimal("3")]})
    path = temp_path
    assert_nil df.write_parquet(path)
    df2 = Polars.read_parquet(path)
    assert_series [1, 2, 3], df2["a"], dtype: Polars::Decimal
  ensure
    Polars::Config.activate_decimals(false)
  end
end

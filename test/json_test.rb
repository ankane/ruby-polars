require_relative "test_helper"

class JsonTest < Minitest::Test
  def test_read_json
    df = Polars.read_json("test/support/data.json")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_json_pathname
    require "pathname"

    df = Polars.read_json(Pathname.new("test/support/data.json"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_json_io
    io = StringIO.new(File.binread("test/support/data.json"))
    df = Polars.read_json(io)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_write_json
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_json(path)
  end

  def test_write_json_row_oriented
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_json(path, row_oriented: true)
    assert_frame df, Polars.read_json(path)
  end

  def test_write_json_pathname
    require "pathname"

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_nil df.write_json(Pathname.new(temp_path))
  end

  def test_write_json_io
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    io = StringIO.new
    df.write_json(io, row_oriented: true)
    io.rewind
    assert_frame df, Polars.read_json(io)
  end

  def test_write_json_nil
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    io = StringIO.new(df.write_json(row_oriented: true))
    assert_frame df, Polars.read_json(io)
  end

  def test_read_ndjson
    df = Polars.read_ndjson("test/support/data.ndjson")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_ndjson_pathname
    require "pathname"

    df = Polars.read_ndjson(Pathname.new("test/support/data.ndjson"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_ndjson_io
    io = StringIO.new(File.binread("test/support/data.ndjson"))
    df = Polars.read_ndjson(io)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_scan_ndjson
    df = Polars.scan_ndjson("test/support/data.ndjson")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_scan_ndjson_io
    io = StringIO.new(File.binread("test/support/data.ndjson"))
    df = Polars.scan_ndjson(io)
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_scan_ndjson_cloud
    skip unless cloud?

    df = Polars.scan_ndjson(cloud_file("data.ndjson"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_write_ndjson
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_ndjson(path)
    assert_frame df, Polars.read_ndjson(path)
  end

  def test_write_ndjson_pathname
    require "pathname"

    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_nil df.write_ndjson(Pathname.new(temp_path))
  end

  def test_write_ndjson_io
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    io = StringIO.new
    df.write_ndjson(io)
    io.rewind
    assert_frame df, Polars.read_ndjson(io)
  end

  def test_write_ndjson_nil
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    io = StringIO.new(df.write_ndjson)
    assert_frame df, Polars.read_ndjson(io)
  end

  def test_sink_ndjson
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.lazy.sink_ndjson(path)
    assert_frame df, Polars.read_ndjson(path)
  end
end

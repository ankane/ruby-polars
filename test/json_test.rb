require_relative "test_helper"

class JsonTest < Minitest::Test
  def test_read_json
    df = Polars.read_json("test/support/data.json")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_json_row_oriented
    df = Polars.read_json("test/support/rows.json")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_write_json
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_json(path)
    assert_frame df, Polars.read_json(path)
  end

  def test_write_json_row_oriented
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_json(path, row_oriented: true)
    assert_frame df, Polars.read_json(path)
  end

  def test_read_ndjson
    df = Polars.read_ndjson("test/support/data.ndjson")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_scan_ndjson
    df = Polars.scan_ndjson("test/support/data.ndjson")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df.collect
  end

  def test_write_ndjson
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_ndjson(path)
    assert_frame df, Polars.read_ndjson(path)
  end

  def test_sink_ndjson
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.lazy.sink_ndjson(path)
    assert_frame df, Polars.read_ndjson(path)
  end
end

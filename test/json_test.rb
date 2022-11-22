require_relative "test_helper"

class JsonTest < Minitest::Test
  def test_json
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_json(path)
    assert_frame df, Polars.read_json(path)
  end

  def test_ndjson
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_ndjson(path)
    assert_frame df, Polars.read_ndjson(path)
  end
end

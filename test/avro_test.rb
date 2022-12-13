require_relative "test_helper"

class AvroTest < Minitest::Test
  def test_read_avro
    df = Polars.read_avro("test/support/data.avro")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_write_avro
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    df.write_avro(path)
    assert_frame df, Polars.read_avro(path)
  end
end

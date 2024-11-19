require_relative "test_helper"

class ArrowTest < Minitest::Test
  def test_c_stream
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    stream = df.arrow_c_stream
    assert_kind_of Integer, stream.to_i
  end
end

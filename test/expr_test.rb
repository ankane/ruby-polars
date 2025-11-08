require_relative "test_helper"

class ExprTest < Minitest::Test
  def test_get
    df = Polars::DataFrame.new({"a" => [1, 2], "b" => [3, 4], "c" => [5, 6]})
    error = assert_raises(TypeError) do
      df.select(Polars.nth(1, "a"))
    end
    assert_equal %!invalid index value: "a"!, error.message
    assert_frame ({"a" => [2]}), df.select(Polars.col("a").get(1))
  end
end

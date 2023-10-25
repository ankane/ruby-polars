require_relative "test_helper"

class SqlTest < Minitest::Test
  def test_works
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3], "b" => ["x", nil, "z"]})
    res = Polars::SQLContext.new(frame: lf).execute(
      "SELECT b, a*2 AS two_a FROM frame WHERE b IS NOT NULL"
    )
    expected = Polars::DataFrame.new({"b" => ["x", "z"], "two_a" => [2, 6]})
    assert_frame expected, res.collect
  end
end

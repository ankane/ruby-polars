require_relative "test_helper"

class StringNameSpaceTest < Minitest::Test
  def test_to_datetime
    s = Polars::Series.new(["2022-08-31 00:00:00.123456789"])
    assert_equal "us", s.str.to_datetime("%Y-%m-%d %H:%M:%S%.f").dtype.time_unit
  end
end

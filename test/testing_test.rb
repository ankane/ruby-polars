require_relative "test_helper"

class TestingTest < Minitest::Test
  def test_assert_frame_equal
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_frame_equal df, df
    error = assert_raises(Polars::AssertionError) do
      assert_frame_not_equal df, df
    end
    assert_equal "frames are equal", error.message
  end

  def test_assert_series_equal
    s = Polars::Series.new([1.0, Float::NAN, Float::INFINITY])
    assert_series_equal s, s
    error = assert_raises(Polars::AssertionError) do
      assert_series_not_equal s, s
    end
    assert_equal "Series are equal", error.message
  end
end

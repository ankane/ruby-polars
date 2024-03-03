require_relative "test_helper"

class TestingTest < Minitest::Test
  def test_assert_frame_equal
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_frame_equal df, df
  end

  def test_assert_frame_equal_different_dtype
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    df2 = Polars::DataFrame.new({"a" => [1.0, 2.0, 3.0]})
    error = assert_raises(Polars::AssertionError) do
      assert_frame_equal df, df2
    end
    assert_match "DataFrames are different (dtypes do not match)", error.message
    assert_frame_equal df, df2, check_dtype: false
  end

  def test_assert_frame_equal_different_columns
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    df2 = Polars::DataFrame.new({"b" => [1, 2, 3]})
    error = assert_raises(Polars::AssertionError) do
      assert_frame_equal df, df2
    end
    assert_equal "columns [\"b\"] in left DataFrames, but not in right", error.message
  end

  def test_assert_frame_not_equal
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    error = assert_raises(Polars::AssertionError) do
      assert_frame_not_equal df, df
    end
    assert_equal "frames are equal", error.message
  end

  def test_assert_series_equal
    s = Polars::Series.new([1, 2, 3])
    assert_series_equal s, s
  end

  def test_assert_series_not_equal
    s = Polars::Series.new([1, 2, 3])
    error = assert_raises(Polars::AssertionError) do
      assert_series_not_equal s, s
    end
    assert_equal "Series are equal", error.message
  end
end

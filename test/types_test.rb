require_relative "test_helper"

class TypesTest < Minitest::Test
  def test_dtypes
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal [Polars::Int64, Polars::Utf8], df.dtypes
  end

  def test_series_dtype_int
    [Polars::Int8, Polars::Int16, Polars::Int32, Polars::Int64].each do |dtype|
      s = Polars::Series.new([1, nil, 3], dtype: dtype)
      assert_series [1, nil, 3], s, dtype: dtype
    end
  end

  def test_series_dtype_uint
    [Polars::UInt8, Polars::UInt16, Polars::UInt32, Polars::UInt64].each do |dtype|
      s = Polars::Series.new([1, nil, 3], dtype: dtype)
      assert_series [1, nil, 3], s, dtype: dtype
    end
  end

  def test_series_dtype_float
    [Polars::Float32, Polars::Float64].each do |dtype|
      s = Polars::Series.new([1.5, nil, 3.5], dtype: dtype)
      assert_series [1.5, nil, 3.5], s, dtype: dtype
    end
  end

  def test_series_dtype_bool
    s = Polars::Series.new([true, nil, false], dtype: Polars::Boolean)
    assert_series [true, nil, false], s, dtype: Polars::Boolean
  end

  def test_series_dtype_str
    s = Polars::Series.new(["a", nil, "c"], dtype: Polars::Utf8)
    assert_series ["a", nil, "c"], s, dtype: Polars::Utf8
  end
end

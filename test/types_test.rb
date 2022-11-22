require_relative "test_helper"

class TypesTest < Minitest::Test
  def test_dtypes
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal [:i64, :str], df.dtypes
  end

  def test_series_dtype_int
    [:i8, :i16, :i32, :i64, :u8, :u16, :u32, :u64].each do |dtype|
      s = Polars::Series.new([1, nil, 3], dtype: dtype)
      assert_series [1, nil, 3], s, dtype: dtype
    end
  end

  def test_series_dtype_float
    [:f32, :f64].each do |dtype|
      s = Polars::Series.new([1.5, nil, 3.5], dtype: dtype)
      assert_series [1.5, nil, 3.5], s, dtype: dtype
    end
  end

  def test_series_dtype_bool
    s = Polars::Series.new([true, nil, false], dtype: :bool)
    assert_series [true, nil, false], s, dtype: :bool
  end

  def test_series_dtype_str
    s = Polars::Series.new(["a", nil, "c"], dtype: :str)
    assert_series ["a", nil, "c"], s, dtype: :str
  end
end

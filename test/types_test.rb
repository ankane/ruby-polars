require_relative "test_helper"

class TypesTest < Minitest::Test
  def test_dtypes
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal [Polars::Int64, Polars::Utf8], df.dtypes
  end

  def test_dtypes_hashes
    row = {
      b: true,
      i: 1,
      f: 1.5,
      s: "one",
      d: Date.today,
      t: Time.now,
      z: Time.now.in_time_zone("Eastern Time (US & Canada)"),
      h: {"f" => 1},
      a: [1, 2, 3]
    }
    df = Polars::DataFrame.new([row])
    schema = df.schema
    assert_equal Polars::Boolean, schema["b"]
    assert_equal Polars::Int64, schema["i"]
    assert_equal Polars::Float64, schema["f"]
    assert_equal Polars::Utf8, schema["s"]
    assert_equal Polars::Date, schema["d"]
    assert_kind_of Polars::Datetime, schema["t"]
    assert_kind_of Polars::Datetime, schema["z"]
    assert_kind_of Polars::Struct, schema["h"]
    assert_kind_of Polars::List, schema["a"]
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
    assert_equal [Encoding::UTF_8, nil, Encoding::UTF_8], s.to_a.map { |v| v&.encoding }
    assert_equal Encoding::UTF_8, s[0].encoding
  end

  def test_series_dtype_binary
    s = Polars::Series.new(["a", nil, "c"], dtype: Polars::Binary)
    assert_series ["a", nil, "c"], s, dtype: Polars::Binary
    assert_equal [Encoding::BINARY, nil, Encoding::BINARY], s.to_a.map { |v| v&.encoding }
    assert_equal Encoding::BINARY, s[0].encoding
  end
end

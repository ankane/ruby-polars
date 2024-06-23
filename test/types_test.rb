require_relative "test_helper"

class TypesTest < Minitest::Test
  def test_dtypes
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal [Polars::Int64, Polars::String], df.dtypes
  end

  def test_dtypes_hashes
    row = {
      b: true,
      i: 1,
      f: 1.5,
      c: BigDecimal("1.5"),
      s: "one",
      n: "two".b,
      d: Date.today,
      t: Time.now,
      z: Time.now.in_time_zone("Eastern Time (US & Canada)"),
      h: {"f" => 1},
      a: [1, 2, 3],
      u: nil
    }
    df = Polars::DataFrame.new([row])
    schema = df.schema
    assert_equal Polars::Boolean, schema["b"]
    assert_equal Polars::Int64, schema["i"]
    assert_equal Polars::Float64, schema["f"]
    assert_equal Polars::Decimal, schema["c"]
    assert_equal Polars::String, schema["s"]
    assert_equal Polars::Binary, schema["n"]
    assert_equal Polars::Date, schema["d"]
    assert_equal Polars::Datetime.new("ns"), schema["t"]
    assert_equal Polars::Datetime.new("ns"), schema["z"]
    assert_equal Polars::Struct.new([Polars::Field.new("f", Polars::Int64)]), schema["h"]
    assert_equal Polars::List.new(Polars::Int64), schema["a"]
    assert_equal Polars::Null, schema["u"]
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

  def test_series_dtype_decimal
    # TODO fix
    skip

    s = Polars::Series.new([BigDecimal("12.3456"), nil, BigDecimal("-0.000078")], dtype: Polars::Decimal)
    assert_series [BigDecimal("12.3456"), nil, BigDecimal("-0.000078")], s, dtype: Polars::Decimal
    assert_equal BigDecimal("12.3456"), s[0]
  end

  def test_series_dtype_boolean
    s = Polars::Series.new([true, nil, false], dtype: Polars::Boolean)
    assert_series [true, nil, false], s, dtype: Polars::Boolean
  end

  def test_series_dtype_utf8
    s = Polars::Series.new(["a", nil, "c"], dtype: Polars::String)
    assert_series ["a", nil, "c"], s, dtype: Polars::String
    assert_equal [Encoding::UTF_8, nil, Encoding::UTF_8], s.to_a.map { |v| v&.encoding }
    assert_equal Encoding::UTF_8, s[0].encoding
  end

  def test_series_dtype_binary
    s = Polars::Series.new(["a", nil, "c"], dtype: Polars::Binary)
    assert_series ["a", nil, "c"], s, dtype: Polars::Binary
    assert_equal [Encoding::BINARY, nil, Encoding::BINARY], s.to_a.map { |v| v&.encoding }
    assert_equal Encoding::BINARY, s[0].encoding
  end

  def test_series_date
    s = Polars::Series.new([Date.new(2022, 1, 1), nil, Date.new(2022, 1, 3)], dtype: Polars::Date)
    assert_series [Date.new(2022, 1, 1), nil, Date.new(2022, 1, 3)], s, dtype: Polars::Date
    assert_equal Date.new(2022, 1, 1), s[0]
  end

  def test_series_dtype_datetime
    s = Polars::Series.new([DateTime.new(2022, 1, 1)], dtype: Polars::Datetime)
    assert_series [Time.utc(2022, 1, 1)], s, dtype: Polars::Datetime.new("ns")
  end

  def test_series_dtype_datetime_time_unit
    s = Polars::Series.new([DateTime.new(2022, 1, 1)], dtype: Polars::Datetime.new("ms"))
    assert_series [Time.utc(2022, 1, 1)], s, dtype: Polars::Datetime.new("ms")
  end

  def test_series_dtype_duration
    s = Polars::Series.new([1e6, 2e6, 3e6], dtype: Polars::Duration)
    assert_series [1, 2, 3], s, dtype: Polars::Duration.new("us")
    assert_series [1e6, 2e6, 3e6], s.dt.microseconds
  end

  def test_series_dtype_duration_time_unit
    s = Polars::Series.new([1e3, 2e3, 3e3], dtype: Polars::Duration.new("ms"))
    assert_series [1, 2, 3], s, dtype: Polars::Duration.new("ms")
    assert_series [1e3, 2e3, 3e3], s.dt.milliseconds
  end

  def test_series_dtype_time
    s = Polars::Series.new([DateTime.new(2022, 1, 1, 12, 34, 56)], dtype: Polars::Time)
    assert_series [Time.utc(2000, 1, 1, 12, 34, 56)], s, dtype: Polars::Time
  end

  def test_series_dtype_categorical
    s = Polars::Series.new(["one", "one", "two"], dtype: Polars::Categorical)
    assert_series ["one", "one", "two"], s, dtype: Polars::Categorical
  end

  def test_series_dtype_list
    s = Polars::Series.new([[1, 2], [3]], dtype: Polars::List)
    assert_series [[1, 2], [3]], s, dtype: Polars::List.new(Polars::Int64)
  end

  def test_series_dtype_list_dtype
    s = Polars::Series.new([[1, 2], [3]], dtype: Polars::List.new(Polars::Int64))
    assert_series [[1, 2], [3]], s, dtype: Polars::List.new(Polars::Int64)
  end

  def test_series_dtype_array
    s = Polars::Series.new([[1, 2], [3, 4]], dtype: Polars::Array)
    assert_series [[1, 2], [3, 4]], s, dtype: Polars::Array.new(2, Polars::Int64)
  end

  def test_series_dtype_array_width
    s = Polars::Series.new([[1, 2], [3, 4]], dtype: Polars::Array.new(2, Polars::Int64))
    assert_series [[1, 2], [3, 4]], s, dtype: Polars::Array.new(2, Polars::Int64)
  end

  def test_series_dtype_array_incompatible_width
    error = assert_raises do
      Polars::Series.new([[1, 2], [3, 4]], dtype: Polars::Array.new(3, Polars::Int64))
    end
    assert_equal "not all elements have the specified width 3", error.message
  end

  def test_series_dtype_struct
    s = Polars::Series.new([{"a" => 1}, {"a" => 2}], dtype: Polars::Struct)
    assert_series [{"a" => 1}, {"a" => 2}], s, dtype: Polars::Struct
  end

  def test_series_dtype_struct_fields
    s = Polars::Series.new([{"a" => 1}, {"a" => 2}], dtype: Polars::Struct.new([Polars::Field.new("a", Polars::Int64)]))
    assert_series [{"a" => 1}, {"a" => 2}], s, dtype: Polars::Struct.new([Polars::Field.new("a", Polars::Int64)])
  end

  def test_series_dtype_object
    s = Polars::Series.new([1, "two"], dtype: Polars::Object)
    assert_series [1, "two"], s, dtype: Polars::Object
  end

  def test_series_dtype_null
    s = Polars::Series.new([nil, nil, nil], dtype: Polars::Null)
    assert_series [nil, nil, nil], s, dtype: Polars::Null
  end

  def test_series_dtype_unknown_int
    s = Polars::Series.new([1, 2, 3], dtype: Polars::Unknown)
    assert_series [1, 2, 3], s, dtype: Polars::Int64
  end

  def test_series_dtype_unknown_utf8
    s = Polars::Series.new(["a", "b", "c"], dtype: Polars::Unknown)
    assert_series ["a", "b", "c"], s, dtype: Polars::String
  end

  def test_object
    s = Polars::Series.new([Object.new])
    GC.start
    assert s.inspect
    assert s.to_a

    df = Polars::DataFrame.new({a: [Object.new]})
    GC.start
    # TODO fix
    # assert df.inspect
    assert df.to_a
  end

  def test_bigdecimal
    assert_bigdecimal "1e-2", "0.01", 2
    assert_bigdecimal "1e-1", "0.1", 1
    assert_bigdecimal "1e0", "1", 0
    assert_bigdecimal "1e1", "10", 0
    assert_bigdecimal "1e2", "100", 0
  end

  def assert_bigdecimal(v, exp, scale)
    b = BigDecimal(v)

    s = Polars::Series.new([b])
    assert_match "\t#{exp}\n", s.inspect
    assert_equal b, s.to_a[0]
    assert_equal b, s[0]
    assert_equal scale, s.dtype.scale

    df = Polars::DataFrame.new([{a: b}])
    assert_match "│ #{exp} ", df.inspect
    assert_equal b, df.to_a[0]["a"]
    assert_equal scale, df.schema["a"].scale
  end
end

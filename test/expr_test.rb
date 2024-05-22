require_relative "test_helper"

class ExprTest < Minitest::Test
  def test_lit
    assert_lit "true", Polars.lit(true)
    assert_lit "false", Polars.lit(false)
    assert_lit "null", Polars.lit(nil)
    assert_lit "dyn int: 1", Polars.lit(1)
    assert_lit "dyn int: 1.strict_cast(Int8)", Polars.lit(1, dtype: Polars::Int8)
    assert_lit "dyn float: 1.5", Polars.lit(1.5)
    assert_lit "String(hello)", Polars.lit("hello")
    assert_lit "[binary value]", Polars.lit("hello".b)
    assert_lit "Series", Polars.lit(Polars::Series.new([1, 2, 3]))
    assert_lit "Series", Polars.lit([1, 2, 3])
    assert_lit "Series", Polars.lit(Numo::NArray.cast([1, 2, 3]))
    assert_lit "dyn int: 1640995200000000000.strict_cast(Datetime(Nanoseconds, None)).strict_cast(Date)", Polars.lit(Date.new(2022, 1, 1))
    assert_lit "dyn int: 1640995200000000000.strict_cast(Datetime(Nanoseconds, None))", Polars.lit(Time.utc(2022, 1, 1))
    assert_lit "dyn int: 1640995200000000000.strict_cast(Datetime(Nanoseconds, None))", Polars.lit(DateTime.new(2022, 1, 1))

    error = assert_raises(ArgumentError) do
      Polars.lit(Object.new)
    end
    assert_match "could not convert value", error.message
  end

  def test_min
    df = Polars::DataFrame.new({"a" => [1, 5, 3], "b" => [4, 2, 6]})
    assert_frame ({"a" => [1]}), df.select(Polars.min("a"))
    assert_frame ({"a" => [1], "b" => [2]}), df.select(Polars.min(["a", "b"]))
  end

  def assert_lit(expected, lit)
    assert_equal expected, lit.inspect
  end
end

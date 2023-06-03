require_relative "test_helper"

class ExprTest < Minitest::Test
  def test_lit
    assert_lit "true", Polars.lit(true)
    assert_lit "false", Polars.lit(false)
    assert_lit "null", Polars.lit(nil)
    assert_lit "1", Polars.lit(1)
    assert_lit "1.5", Polars.lit(1.5)
    assert_lit "Utf8(hello)", Polars.lit("hello")
    assert_lit "[binary value]", Polars.lit("hello".b)
    assert_lit "Series", Polars.lit(Polars::Series.new([1, 2, 3]))

    error = assert_raises(ArgumentError) do
      Polars.lit(Object.new)
    end
    assert_match "could not convert value", error.message
  end

  def assert_lit(expected, lit)
    assert_equal expected, lit.inspect
  end
end

require_relative "test_helper"

class ExprTest < Minitest::Test
  def test_lit
    Polars.lit(true)
    Polars.lit(false)
    Polars.lit(nil)
    Polars.lit(1)
    Polars.lit(1.5)
    Polars.lit("hello")

    error = assert_raises(ArgumentError) do
      Polars.lit(Object.new)
    end
    assert_match "could not convert value", error.message
  end
end

require_relative "test_helper"

class ArrayExprTest < Minitest::Test
  def test_min
    assert_expr arr_expr.min
  end

  def test_max
    assert_expr arr_expr.max
  end

  def test_sum
    assert_expr arr_expr.sum
  end

  def arr_expr
    Polars.col("a").arr
  end
end

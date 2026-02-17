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

  def test_to_struct
    s = Polars::Series.new("n", [[0, 1, 2], [3, 4, 5]], dtype: Polars::Array.new(Polars::Int8, 3))
    df = Polars::DataFrame.new([s])
    expr = Polars.col("n").arr.to_struct(fields: ->(i) { "f#{i}" })
    GC.start
    assert_equal ["f0", "f1", "f2"], df.select(expr).to_series.struct.fields
  end

  private

  def arr_expr
    Polars.col("a").arr
  end
end

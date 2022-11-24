require_relative "test_helper"

class CatExprTest < Minitest::Test
  def test_set_ordering
    assert_expr cat_expr.set_ordering("lexical")
  end

  def cat_expr
    Polars.col("a").cat
  end
end

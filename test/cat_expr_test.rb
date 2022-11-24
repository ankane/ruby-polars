require_relative "test_helper"

class CatExprTest < Minitest::Test
  def test_set_ordering
    assert_kind_of Polars::Expr, Polars.col("a").cat.set_ordering("lexical")
  end
end

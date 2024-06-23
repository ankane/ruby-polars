require_relative "test_helper"

class CatExprTest < Minitest::Test
  def cat_expr
    Polars.col("a").cat
  end
end

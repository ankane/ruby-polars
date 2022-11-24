require_relative "test_helper"

class MetaExprTest < Minitest::Test
  def test_equal
    assert(meta_expr == meta_expr)
  end

  def test_not_equal
    refute(meta_expr != meta_expr)
  end

  def test_pop
    assert_empty meta_expr.pop
  end

  def test_root_names
    assert_equal ["a"], meta_expr.root_names
  end

  def test_output_name
    assert_equal "a", meta_expr.output_name
  end

  def test_undo_aliases
    assert_equal Polars.col("a"), meta_expr.undo_aliases
  end

  def meta_expr
    Polars.col("a").meta
  end
end

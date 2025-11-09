require_relative "test_helper"

class ListExprTest < Minitest::Test
  def test_lengths
    assert_expr list_expr.lengths
  end

  def test_sum
    assert_expr list_expr.sum
  end

  def test_max
    assert_expr list_expr.max
  end

  def test_min
    assert_expr list_expr.min
  end

  def test_mean
    assert_expr list_expr.mean
  end

  def test_sort
    assert_expr list_expr.sort
  end

  def test_reverse
    assert_expr list_expr.reverse
  end

  def test_unique
    assert_expr list_expr.unique
  end

  def test_get
    assert_expr list_expr.get(0)
  end

  def test_getitem
    assert_expr list_expr[0]
  end

  def test_first
    assert_expr list_expr.first
  end

  def test_last
    assert_expr list_expr.last
  end

  def test_contains
    assert_expr list_expr.contains(0)
  end

  def test_join
    assert_expr list_expr.join(",")
  end

  def test_arg_min
    assert_expr list_expr.arg_min
  end

  def test_arg_max
    assert_expr list_expr.arg_max
  end

  def test_diff
    assert_expr list_expr.diff
  end

  def test_shift
    assert_expr list_expr.shift
  end

  def test_slice
    assert_expr list_expr.slice(0)
  end

  def test_head
    assert_expr list_expr.head
  end

  def test_tail
    assert_expr list_expr.tail
  end

  def test_eval
    rank_pct = Polars.element.rank(descending: true) / Polars.col("").count
    assert_expr list_expr.eval(rank_pct)
  end

  def list_expr
    Polars.col("a").list
  end
end

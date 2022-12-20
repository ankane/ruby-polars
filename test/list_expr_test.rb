require_relative "test_helper"

class ListExprTest < Minitest::Test
  def test_lengths
    assert_expr arr_expr.lengths
  end

  def test_sum
    assert_expr arr_expr.sum
  end

  def test_max
    assert_expr arr_expr.max
  end

  def test_min
    assert_expr arr_expr.min
  end

  def test_mean
    assert_expr arr_expr.mean
  end

  def test_sort
    assert_expr arr_expr.sort
  end

  def test_reverse
    assert_expr arr_expr.reverse
  end

  def test_unique
    assert_expr arr_expr.unique
  end

  def test_get
    assert_expr arr_expr.get(0)
  end

  def test_getitem
    assert_expr arr_expr[0]
  end

  def test_first
    assert_expr arr_expr.first
  end

  def test_last
    assert_expr arr_expr.last
  end

  def test_contains
    assert_expr arr_expr.contains(0)
  end

  def test_join
    assert_expr arr_expr.join(",")
  end

  def test_arg_min
    assert_expr arr_expr.arg_min
  end

  def test_arg_max
    assert_expr arr_expr.arg_max
  end

  def test_diff
    assert_expr arr_expr.diff
  end

  def test_shift
    assert_expr arr_expr.shift
  end

  def test_slice
    assert_expr arr_expr.slice(0)
  end

  def test_head
    assert_expr arr_expr.head
  end

  def test_tail
    assert_expr arr_expr.tail
  end

  def test_eval
    rank_pct = Polars.element.rank(reverse: true) / Polars.col("").count
    assert_expr arr_expr.eval(rank_pct)
  end

  def arr_expr
    Polars.col("a").arr
  end
end

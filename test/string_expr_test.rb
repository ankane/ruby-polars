require_relative "test_helper"

class StringExprTest < Minitest::Test
  # def test_strptime
  # end

  def test_lengths
    assert_expr str_expr.lengths
  end

  def test_n_chars
    assert_expr str_expr.n_chars
  end

  def test_concat
    assert_expr str_expr.concat
  end
  def test_to_uppercase
    assert_expr str_expr.to_uppercase
  end

    def test_to_lowercase
    assert_expr str_expr.to_lowercase
  end

  def str_expr
    Polars.col("a").str
  end
end

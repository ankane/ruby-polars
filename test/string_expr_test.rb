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

  # def test_strip
  # end

  # def test_lstrip
  # end

  # def test_rstrip
  # end

  # def test_zfill
  # end

  # def test_ljust
  # end

  # def test_rjust
  # end

  # def test_contains
  # end

  # def test_ends_with
  # end

  # def test_starts_with
  # end

  # def test_json_path_match
  # end

  # def test_decode
  # end

  # def test_encode
  # end

  # def test_extract
  # end

  # def test_extract_all
  # end

  # def test_count_match
  # end

  # def test_split
  # end

  # def test_split_exact
  # end

  # def test_splitn
  # end

  # def test_replace
  # end

  # def test_replace_all
  # end

  # def test_slice
  # end

  def str_expr
    Polars.col("a").str
  end
end

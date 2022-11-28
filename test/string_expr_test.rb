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

  def test_strip
    assert_expr str_expr.strip
  end

  def test_lstrip
    assert_expr str_expr.lstrip
  end

  def test_rstrip
    assert_expr str_expr.rstrip
  end

  def test_zfill
    assert_expr str_expr.zfill(1)
  end

  def test_ljust
    assert_expr str_expr.ljust(1)
  end

  def test_rjust
    assert_expr str_expr.rjust(1)
  end

  def test_contains
    assert_expr str_expr.contains("pattern")
  end

  def test_ends_with
    assert_expr str_expr.ends_with("sub")
  end

  def test_starts_with
    assert_expr str_expr.starts_with("sub")
  end

  # def test_json_path_match
  # end

  # def test_decode
  # end

  # def test_encode
  # end

  def test_extract
    assert_expr str_expr.extract("pattern")
  end

  def test_extract_all
    assert_expr str_expr.extract_all("pattern")
  end

  def test_count_match
    assert_expr str_expr.count_match("pattern")
  end

  def test_split
    assert_expr str_expr.split("by")
    assert_expr str_expr.split("by", inclusive: true)
  end

  def test_split_exact
    assert_expr str_expr.split_exact("by", 1)
    assert_expr str_expr.split_exact("by", 1, inclusive: true)
  end

  def test_splitn
    assert_expr str_expr.splitn("by", 1)
  end

  def test_replace
    assert_expr str_expr.replace("pattern", "value")
  end

  def test_replace_all
    assert_expr str_expr.replace_all("pattern", "value")
  end

  def test_slice
    assert_expr str_expr.slice(1)
  end

  def str_expr
    Polars.col("a").str
  end
end

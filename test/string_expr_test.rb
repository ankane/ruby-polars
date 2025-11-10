require_relative "test_helper"

class StringExprTest < Minitest::Test
  def test_len_bytes
    assert_expr str_expr.len_bytes
  end

  def test_len_chars
    assert_expr str_expr.len_chars
  end

  def test_join
    assert_expr str_expr.join
  end

  def test_to_uppercase
    assert_expr str_expr.to_uppercase
  end

  def test_to_lowercase
    assert_expr str_expr.to_lowercase
  end

  def test_strip_chars
    assert_expr str_expr.strip_chars
  end

  def test_strip_chars_start
    assert_expr str_expr.strip_chars_start
  end

  def test_strip_chars_end
    assert_expr str_expr.strip_chars_end
  end

  def test_zfill
    assert_expr str_expr.zfill(1)
  end

  def test_pad_start
    assert_expr str_expr.pad_start(1)
  end

  def test_pad_end
    assert_expr str_expr.pad_end(1)
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

  def test_extract
    assert_expr str_expr.extract("pattern")
  end

  def test_extract_all
    assert_expr str_expr.extract_all("pattern")
  end

  def test_count_matches
    assert_expr str_expr.count_matches("pattern")
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

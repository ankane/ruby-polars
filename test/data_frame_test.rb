require_relative "test_helper"

class DataFrameTest < Minitest::Test
  def test_new_array_series
    df = Polars::DataFrame.new([
      Polars::Series.new("a", [1, 2, 3]),
      Polars::Series.new("b", ["one", "two", "three"]),
    ])
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_new_hash
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal ["a", "b"], df.columns
  end

  def test_new_hash_symbol_keys
    df = Polars::DataFrame.new({a: [1, 2, 3], b: ["one", "two", "three"]})
    assert_equal ["a", "b"], df.columns
  end

  def test_new_series
    df = Polars::DataFrame.new(Polars::Series.new("a", [1, 2, 3]))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_frame expected, df
  end

  def test_shape
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal [3, 2], df.shape
  end

  def test_height
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal 3, df.height
  end

  def test_width
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal 2, df.width
  end

  def test_columns
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal ["a", "b"], df.columns
  end

  def test_set_columns
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df.columns = ["c", "d"]
    assert_equal ["c", "d"], df.columns
  end

  def test_dtypes
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal [:i64, :str], df.dtypes
  end

  def test_to_s
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_match "│ a   │", df.to_s
  end

  def test_inspect
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_match "│ a   │", df.inspect
  end

  def test_include
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert df.include?("a")
    refute df.include?("c")
  end

  def test_reverse
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    expected = Polars::DataFrame.new({"a" => [3, 2, 1], "b" => ["three", "two", "one"]})
    assert_frame expected, df.reverse
  end

  def test_rename
    df = Polars::DataFrame.new({"a" => [1], "b" => [2]})
    assert_equal ["c", "b"], df.rename({"a" => "c"}).columns
  end

  def test_slice
    df = Polars::DataFrame.new({"a" => 1..10})
    assert_series 6..10, df.slice(5)["a"]
    assert_series 6..8, df.slice(5, 3)["a"]
  end

  def test_head
    df = Polars::DataFrame.new({"a" => 1..20})
    assert_series 1..5, df.head["a"]
    assert_series [1, 2, 3], df.head(3)["a"]
  end

  def test_tail
    df = Polars::DataFrame.new({"a" => 1..20})
    assert_series 16..20, df.tail["a"]
    assert_series [18, 19, 20], df.tail(3)["a"]
  end
end

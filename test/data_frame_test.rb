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
    assert_equal 3, df.height
    assert_equal 2, df.width
    assert_equal [3, 2], df.shape
    assert_equal ["a", "b"], df.columns
    assert df.include?("a")
    refute df.include?("c")
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

  def test_to_s
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_match "│ a   │", df.to_s
  end

  def test_inspect
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_match "│ a   │", df.inspect
  end

  def test_head
    df = Polars::DataFrame.new({"a" => 1..20})
    assert_series (1..5).to_a, df.head["a"]
    assert_series [1, 2, 3], df.head(3)["a"]
  end

  def test_tail
    df = Polars::DataFrame.new({"a" => 1..20})
    assert_series (16..20).to_a, df.tail["a"]
    assert_series [18, 19, 20], df.tail(3)["a"]
  end
end

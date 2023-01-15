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
    assert_equal [Polars::Int64, Polars::Utf8], df.dtypes
  end

  def test_schema
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    expected = {"a" => Polars::Int64, "b" => Polars::Utf8}
    assert_equal expected, df.schema
  end

  def test_comp_data_frame
    a = Polars::DataFrame.new({"a" => [1, 2, 3, 4]})
    b = Polars::DataFrame.new({"a" => [0, 2, 3, 5]})
    assert_frame ({"a" => [false, true, true, false]}), a == b
    assert_frame ({"a" => [true, false, false, true]}), a != b
    assert_frame ({"a" => [true, false, false, false]}), a > b
    assert_frame ({"a" => [false, false, false, true]}), a < b
    assert_frame ({"a" => [true, true, true, false]}), a >= b
    assert_frame ({"a" => [false, true, true, true]}), a <= b
  end

  def test_comp_scalar
    a = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_frame ({"a" => [false, true, false]}), a == 2
    assert_frame ({"a" => [true, false, true]}), a != 2
    assert_frame ({"a" => [false, false, true]}), a > 2
    assert_frame ({"a" => [true, false, false]}), a < 2
    assert_frame ({"a" => [false, true, true]}), a >= 2
    assert_frame ({"a" => [true, true, false]}), a <= 2
  end

  def test_arithmetic_data_frame
    a = Polars::DataFrame.new({"a" => [10, 20, 30]})
    b = Polars::DataFrame.new({"a" => [5, 10, 15]})
    assert_frame ({"a" => [15, 30, 45]}), a + b
    assert_frame ({"a" => [5, 10, 15]}), a - b
    assert_frame ({"a" => [50, 200, 450]}), a * b
    assert_frame ({"a" => [2, 2, 2]}), a / b
    assert_frame ({"a" => [0, 0, 0]}), a % b
  end

  def test_arithmetic_series
    a = Polars::DataFrame.new({"a" => [10, 20, 30]})
    b = Polars::Series.new("b", [5, 10, 15])
    assert_frame ({"a" => [15, 30, 45]}), a + b
    assert_frame ({"a" => [5, 10, 15]}), a - b
    assert_frame ({"a" => [50, 200, 450]}), a * b
    assert_frame ({"a" => [2, 2, 2]}), a / b
    assert_frame ({"a" => [0, 0, 0]}), a % b
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

  def test_to_h
    data = {"a" => [1, 2, 3], "b" => ["one", "two", "three"]}
    df = Polars::DataFrame.new(data)
    assert_equal data, df.to_h(as_series: false)
  end

  def test_to_series
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_series df["a"], df.to_series
    assert_series df["b"], df.to_series(-1)
  end

  # write_json tested in json_test

  # write_ndjson tested in json_test

  # write_csv tested in csv_test

  # write_parquet tested in parquet_test

  def test_estimated_size
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_in_delta df.estimated_size("kb"), df.estimated_size / 1024.0
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

  def test_insert_at_idx
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df.insert_at_idx(1, Polars::Series.new("c", [4, 5, 6]))
    assert_equal ["a", "c", "b"], df.columns
  end

  def test_filter
  end

  def test_describe
    df = Polars::DataFrame.new({
      "a" => [1.0, 2.8, 3.0],
      "b" => [4, 5, nil],
      "c" => [true, false, true],
      "d" => [nil, "b", "c"],
      "e" => ["usd", "eur", nil]
    })
    assert df.describe
  end

  def test_sort
  end

  def test_frame_equal
  end

  def test_slice
    df = Polars::DataFrame.new({"a" => 1..10})
    assert_series 6..10, df.slice(5)["a"]
    assert_series 6..8, df.slice(5, 3)["a"]
  end

  def test_limit
    df = Polars::DataFrame.new({"a" => 1..20})
    assert_series 1..5, df.limit["a"]
    assert_series [1, 2, 3], df.limit(3)["a"]
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

  def test_groupby
  end

  def test_join
  end

  def test_with_column
  end

  def test_get_columns
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_kind_of Array, df.get_columns
  end

  def test_get_column
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_series [1, 2, 3], df.get_column("a")
  end

  def test_fill_null
  end

  def test_fill_nan
    df = Polars::DataFrame.new({"a" => [1.0, Float::NAN, 3]})
    assert_series [1, 99, 3], df.fill_nan(99)["a"]
  end

  def test_is_duplicated
    df = Polars::DataFrame.new({"a" => [1, 2, 3, 1], "b" => ["x", "y", "z", "x"]})
    assert_series [true, false, false, true], df.is_duplicated
  end

  def test_is_unique
    df = Polars::DataFrame.new({"a" => [1, 2, 3, 1], "b" => ["x", "y", "z", "x"]})
    assert_series [false, true, true, false], df.is_unique
  end

  def test_lazy
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_kind_of Polars::LazyFrame, df.lazy
  end

  def test_select
  end

  def test_with_columns
  end

  def test_n_chunks
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal 1, df.n_chunks
    assert_equal [1, 1], df.n_chunks(strategy: "all")
  end

  def test_max
    df = Polars::DataFrame.new({"a" => [1, 5, 3], "b" => [4, 2, 6]})
    assert_frame ({"a" => [5], "b" => [6]}), df.max
    assert_series [4, 5, 6], df.max(axis: 1)
  end

  def test_min
    df = Polars::DataFrame.new({"a" => [1, 5, 3], "b" => [4, 2, 6]})
    assert_frame ({"a" => [1], "b" => [2]}), df.min
    assert_series [1, 2, 3], df.min(axis: 1)
  end

  def test_sum
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert df.sum
    assert df.sum(axis: 1)
  end

  def test_mean
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert df.mean
    assert df.mean(axis: 1)
  end

  def test_std
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_in_delta 1, df.std["a"][0]
    assert_in_delta 0.816497, df.std(ddof: 0)["a"][0]
  end

  def test_var
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_in_delta 1, df.var["a"][0]
    assert_in_delta 0.666667, df.var(ddof: 0)["a"][0]
  end

  def test_median
    df = Polars::DataFrame.new({"a" => [1, 2, 5], "b" => ["one", "two", "three"]})
    assert_frame ({"a" => [2], "b" => [nil]}), df.median
  end

  def test_rechunk
  end

  def test_null_count
  end
end

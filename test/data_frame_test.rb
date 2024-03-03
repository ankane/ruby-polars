require_relative "test_helper"

class DataFrameTest < Minitest::Test
  def test_new_array_hashes
    df = Polars::DataFrame.new([
      {"a" => 1, "b" => "one"},
      {"a" => 2, "b" => "two"},
      {"a" => 3, "b" => "three"}
    ])
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_new_array_hashes_symbol_keys
    df = Polars::DataFrame.new([
      {a: 1, b: "one"},
      {a: 2, b: "two"},
      {a: 3, b: "three"}
    ])
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_new_array_series
    df = Polars::DataFrame.new([
      Polars::Series.new("a", [1, 2, 3]),
      Polars::Series.new("b", ["one", "two", "three"]),
    ])
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_new_array_schema
    df = Polars::DataFrame.new([{"a" => DateTime.new(2022, 1, 1)}], schema: {"a" => Polars::Datetime})
    assert_kind_of Polars::Datetime, df["a"].dtype
    assert_equal "us", df["a"].dtype.time_unit
  end

  def test_new_array_schema_time_unit
    df = Polars::DataFrame.new([{"a" => DateTime.new(2022, 1, 1)}], schema: {"a" => Polars::Datetime.new("ms")})
    assert_kind_of Polars::Datetime, df["a"].dtype
    assert_equal "ms", df["a"].dtype.time_unit
  end

  def test_new_hash
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_equal ["a", "b"], df.columns
  end

  def test_new_hash_symbol_keys
    df = Polars::DataFrame.new({a: [1, 2, 3], b: ["one", "two", "three"]})
    assert_equal ["a", "b"], df.columns
  end

  def test_new_hash_unsupported_key
    error = assert_raises(TypeError) do
      Polars::DataFrame.new({Object.new => 1..3})
    end
    assert_equal "no implicit conversion of Object into String", error.message
  end

  def test_new_hash_different_sizes
    error = assert_raises do
      Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [1, 2]})
    end
    assert_match "lengths don't match", error.message
  end

  def test_new_hash_scalar
    df = Polars::DataFrame.new({"a" => 1, "b" => [1, 2, 3]})
    assert_series [1, 1, 1], df["a"]
  end

  def test_new_hash_scalar_empty_series
    df = Polars::DataFrame.new({"a" => [], "b" => Polars::Series.new([], dtype: Polars::String)})
    assert_equal ["a", "b"], df.columns
    assert_equal [Polars::Float32, Polars::String], df.dtypes
  end

  def test_new_hash_scalar_nil_empty_series
    df = Polars::DataFrame.new({"a" => nil, "b" => Polars::Series.new([], dtype: Polars::String)})
    assert_equal ["a", "b"], df.columns
    # same behavior as Python
    assert_equal [Polars::Float32, Polars::Float32], df.dtypes
  end

  def test_new_hash_schema
    df = Polars::DataFrame.new({"a" => [DateTime.new(2022, 1, 1)]}, schema: {"a" => Polars::Datetime})
    assert_kind_of Polars::Datetime, df["a"].dtype
    assert_equal "ns", df["a"].dtype.time_unit
  end

  def test_new_hash_schema_time_unit
    df = Polars::DataFrame.new({"a" => [DateTime.new(2022, 1, 1)]}, schema: {"a" => Polars::Datetime.new("ms")})
    assert_kind_of Polars::Datetime, df["a"].dtype
    assert_equal "ms", df["a"].dtype.time_unit
  end

  def test_new_series
    df = Polars::DataFrame.new(Polars::Series.new("a", [1, 2, 3]))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_frame expected, df
  end

  def test_new_unsupported
    error = assert_raises(ArgumentError) do
      Polars::DataFrame.new(Object.new)
    end
    assert_equal "DataFrame constructor called with unsupported type; got Object", error.message
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
    assert_equal [Polars::Int64, Polars::String], df.dtypes
  end

  def test_schema
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    expected = {"a" => Polars::Int64, "b" => Polars::String}
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

  def test_comp_data_frame_different_schema
    a = Polars::DataFrame.new({"a" => [1]})
    b = Polars::DataFrame.new({"b" => [1]})
    error = assert_raises(ArgumentError) do
      a == b
    end
    assert_match "DataFrame columns do not match", error.message
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

  def test_get
    a = Polars::Series.new("a", [1, 2, 3])
    df = Polars::DataFrame.new([a])
    assert_equal a, df["a"]
    assert_equal a, df[:a]
    assert_frame({"a" => [2, 3]}, df[Polars.col("a") > 1])
    assert_frame({"a" => [2, 3]}, df[df["a"] > 1])
    assert_frame({"a" => [2, 3]}, df[[1, 2]])
    assert_frame({"a" => [2, 3]}, df[1..2])
    assert_frame({"a" => [2]}, df[1...2])
  end

  def test_set
    a = [1, 2, 3]
    b = ["one", "two", "three"]
    df = Polars::DataFrame.new({"a" => a})

    df["a"] = Polars::Series.new(b)
    assert_series b, df["a"]

    df["b"] = b
    assert_series b, df["b"]

    df[:c] = 1
    assert_series [1, 1, 1], df["c"]

    df[1, "c"] = 2
    df[2, :c] = 3
    assert_series [1, 2, 3], df["c"]
    assert_equal 1, df[0, "c"]
    assert_equal 2, df[1, :c]
    assert_equal 3, df[2, "c"]
    error = assert_raises do
      df[3, "c"]
    end
    assert_equal "index 3 is out of bounds for sequence of length 3", error.message

    error = assert_raises(ArgumentError) do
      df[] = 1
    end
    assert_equal "wrong number of arguments (given 1, expected 2..3)", error.message

    error = assert_raises do
      df["d"] = [1, 2]
    end
    assert_match "lengths don't match", error.message
  end

  def test_to_h
    data = {"a" => [1, 2, 3], "b" => ["one", "two", "three"]}
    df = Polars::DataFrame.new(data)
    assert_equal data, df.to_h(as_series: false)
  end

  def test_to_a
    data = [
      {"a" => 1, "b" => "one"},
      {"a" => 2, "b" => "two"},
      {"a" => 3, "b" => "three"}
    ]
    df = Polars::DataFrame.new(data)
    assert_equal data, df.to_a
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

  def test_insert_column
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df.insert_column(1, Polars::Series.new("c", [4, 5, 6]))
    assert_equal ["a", "c", "b"], df.columns
  end

  def test_filter
    a = Polars::Series.new("a", [1, 2, 3])
    df = Polars::DataFrame.new([a])
    assert_frame({"a" => [2, 3]}, df.filter(Polars.col("a") > 1))
    assert_frame({"a" => [2, 3]}, df.filter(df["a"] > 1))
    assert_frame({"a" => [1]}, df.filter(!(df["a"] > 1)))
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
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame ({"a" => [1, 3, 2], "b" => ["one", "three", "two"]}), df.sort("b")
    assert_frame ({"a" => [1, 2, 3], "b" => ["one", "two", "three"]}), df
    df.sort!("b")
    assert_frame ({"a" => [1, 3, 2], "b" => ["one", "three", "two"]}), df
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

  def test_group_by
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df.group_by("a").count
    df.group_by(:a).count
    df.group_by(["a", "b"]).count
    df.group_by([:a, :b]).count
    df.groupby("a").count
    df.group("a").count
  end

  def test_join
    df = Polars::DataFrame.new({
      a: [1, 2, 3],
      b: ["one", "two", "three"]
    })

    other_df = Polars::DataFrame.new({
      a: [1, 1, 2],
      c: ["c1", "c2", "c3"]
    })

    expected = Polars::DataFrame.new({
      a: [1, 1, 2],
      b: ["one", "one", "two"],
      c: ["c1", "c2", "c3"]
    })

    assert_equal expected, df.join(other_df, on: "a")
    assert_equal expected, df.join(other_df, on: :a)
    assert_equal expected, df.join(other_df, on: ["a"])
    assert_equal expected, df.join(other_df, on: [:a])
  end

  def test_join_nulls
    df1 = Polars::DataFrame.new({"a" => [1, 2, nil], "b" => [4, 4, 4]})
    df2 = Polars::DataFrame.new({"a" => [nil, 2, 3], "c" => [5, 5, 5]})
    df3 = df1.join(df2, on: "a", how: "inner")
    assert_frame Polars::DataFrame.new({"a" => [2], "b" => [4], "c" => [5]}), df3
    df4 = df1.join(df2, on: "a", how: "inner", join_nulls: true)
    assert_frame Polars::DataFrame.new({"a" => [nil, 2], "b" => [4, 4], "c" => [5, 5]}), df4
  end

  def test_join_outer
    df1 = Polars::DataFrame.new({"L1" => ["a", "b", "c"], "L2" => [1, 2, 3]})
    df2 = Polars::DataFrame.new({"L1" => ["a", "c", "d"], "R2" => [7, 8, 9]})
    df3 = df1.join(df2, on: "L1", how: "outer")
    expected =
      Polars::DataFrame.new({
        "L1" => ["a", "b", "c", nil],
        "L2" => [1, 2, 3, nil],
        "L1_right" => ["a", nil, "c", "d"],
        "R2" => [7, nil, 8, 9]
      })
    assert_frame expected, df3.sort("L1", nulls_last: true)
    expected =
      Polars::DataFrame.new({
        "L1" => ["a", "c", "d", "b"],
        "L2" => [1, 3, nil, 2],
        "R2" => [7, 8, 9, nil]
      })
    df4 = df1.join(df2, on: "L1", how: "outer_coalesce")
    assert_frame expected, df4
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

  def test_drop_in_place
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_series [1, 2, 3], df.drop_in_place("a")
    assert_frame ({"b" => ["one", "two", "three"]}), df
    assert_nil df.delete("c")
  end

  def test_rechunk
  end

  def test_null_count
  end

  def test_count
    df = Polars::DataFrame.new({"a" => [1, 2, nil]})
    assert_frame Polars::DataFrame.new({"a" => [2]}), df.select(Polars.col("a").count), check_dtype: false
    assert_frame Polars::DataFrame.new({"a" => [3]}), df.select(Polars.col("a").len), check_dtype: false
  end

  def test_replace
    data = {"a" => [1, 2, 2, 3], "b" => [1.5, 2.5, 5.0, 1.0]}
    df = Polars::DataFrame.new(data, schema: {"a" => Polars::Int8, "b" => Polars::Float64})
    expected = Polars::DataFrame.new({"a" => [1, 100, 100, 3]})
    assert_frame expected, df.select(Polars.col("a").replace({2 => 100}))
    expected = Polars::DataFrame.new({"a" => [1.5, 100.0, 100.0, 1.0]})
    assert_frame expected, df.select(Polars.col("a").replace({2 => 100}, default: Polars.col("b")))
  end
end

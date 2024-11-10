require_relative "test_helper"

class LazyFrameTest < Minitest::Test
  def test_to_s
    df = Polars::DataFrame.new({"a" => [1, 2, 3]}).lazy
    assert_match "naive plan:", df.select("a").to_s
  end

  def test_select
    df = Polars::DataFrame.new(
      {
        "foo" => [1, 2, 3],
        "bar" => [6, 7, 8],
        "ham" => ["a", "b", "c"]
      }
    ).lazy
    df.select("foo").collect
    df.select(["foo", "bar"]).collect
    df.select(Polars.col("foo") + 1).collect
  end

  def test_unnest
    df = (
      Polars::DataFrame.new(
        {
          "before" => ["foo", "bar"],
          "t_a" => [1, 2],
          "t_b" => ["a", "b"],
          "t_c" => [true, nil],
          "t_d" => [[1, 2], [3]],
          "after" => ["baz", "womp"]
        }
      )
      .lazy
      .select(
        ["before", Polars.struct(Polars.col("^t_.$")).alias("t_struct"), "after"]
      )
    )
    df.fetch
    df.unnest("t_struct").fetch
  end

  def test_write_json
    df = Polars::DataFrame.new(
      {
        "foo" => [1, 2, 3],
        "bar" => [6, 7, 8],
        "ham" => ["a", "b", "c"]
      }
    ).lazy
    path = temp_path
    df.select("foo").write_json(path)
    assert_frame df.select("foo").collect, Polars::LazyFrame.read_json(path).collect
  end

  def test_pearson_corr
    df = Polars::DataFrame.new({
        a: [1, 2, 3, 4],
        b: [2, 4, 6, 7]
      })
      .lazy
      .select(
        Polars.corr("a", "b", method: "pearson")
      )
      .collect
    assert_in_delta 0.989778, df["a"][0]
  end

  def test_describe_optimized_plan
    df = Polars::DataFrame.new({"a" => [1, 2, 3]}).lazy
    assert_match "PROJECT", df.select("a").describe_optimized_plan
  end

  def test_concat
    df1 = Polars::LazyFrame.new({"a" => [1], "b" => [3]})
    df2 = Polars::LazyFrame.new({"a" => [2], "b" => [4]})
    Polars.concat([df1, df2])
    Polars.concat([df1, df2], how: "vertical_relaxed")
    Polars.concat([df1, df2], how: "diagonal")
  end
end

require_relative "test_helper"

class LazyFrameTest < Minitest::Test
  def test_to_s
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3]})
    assert_match "naive plan:", lf.select("a").to_s
  end

  def test_select
    lf = Polars::LazyFrame.new(
      {
        "foo" => [1, 2, 3],
        "bar" => [6, 7, 8],
        "ham" => ["a", "b", "c"]
      }
    )
    lf.select("foo").collect
    lf.select(["foo", "bar"]).collect
    lf.select(Polars.col("foo") + 1).collect
  end

  def test_unnest
    lf = (
      Polars::LazyFrame.new(
        {
          "before" => ["foo", "bar"],
          "t_a" => [1, 2],
          "t_b" => ["a", "b"],
          "t_c" => [true, nil],
          "t_d" => [[1, 2], [3]],
          "after" => ["baz", "womp"]
        }
      )
      .select(
        ["before", Polars.struct(Polars.col("^t_.$")).alias("t_struct"), "after"]
      )
    )
    lf.collect
    lf.unnest("t_struct").collect
  end

  def test_unpivot
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3]})
    assert_frame ({"variable" => ["a", "a", "a"], "value" => [1, 2, 3]}), lf.unpivot.sort("value").collect
    assert lf.unpivot([]).collect.is_empty
  end

  def test_serialize
    lf = Polars::LazyFrame.new(
      {
        "foo" => [1, 2, 3],
        "bar" => [6, 7, 8],
        "ham" => ["a", "b", "c"]
      }
    )
    path = temp_path
    _, stderr = capture_io do
      lf.select("foo").serialize(path, format: "json")
    end
    assert_match "'json' serialization format of LazyFrame is deprecated", stderr
    assert_frame lf.select("foo").collect, Polars::LazyFrame.deserialize(path, format: "json").collect
  end

  def test_pearson_corr
    lf = Polars::LazyFrame.new({
        a: [1, 2, 3, 4],
        b: [2, 4, 6, 7]
      })
      .select(
        Polars.corr("a", "b", method: "pearson")
      )
    assert_in_delta 0.989778, lf.collect["a"][0]
  end

  def test_explain
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3]})
    assert_match "PROJECT", lf.select("a").explain
    assert_match "PROJECT", lf.select("a").explain(optimized: true)
    assert_match "PROJECT", lf.select("a").explain(format: "tree")
    assert_match "PROJECT", lf.select("a").explain(format: "tree", optimized: true)
  end

  def test_explain_all
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3]})
    assert_match "PROJECT", Polars.explain_all([lf.select("a")])
  end

  def test_collect_background
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3]})
    assert_frame lf.collect, lf.collect(background: true).fetch_blocking
  end

  def test_concat
    lf1 = Polars::LazyFrame.new({"a" => [1], "b" => [3]})
    lf2 = Polars::LazyFrame.new({"a" => [2], "b" => [4]})
    Polars.concat([lf1, lf2])
    Polars.concat([lf1, lf2], how: "vertical_relaxed")
    Polars.concat([lf1, lf2], how: "diagonal")
  end

  def test_concat_horizontal
    lf1 = Polars::LazyFrame.new({"a" => [1, 2]})
    lf2 = Polars::LazyFrame.new({"b" => [3, 4]})
    lf = Polars.concat([lf1, lf2], how: "horizontal")
    assert_frame ({"a" => [1, 2], "b" => [3, 4]}), lf.collect
  end

  def test_fill_null
    lf = Polars::LazyFrame.new({a: [1, nil], b: [nil, "two"]})
    assert_frame ({"a" => [1, 2], "b" => [nil, "two"]}), lf.fill_null(2).collect
    assert_frame ({"a" => [1, nil], "b" => ["one", "two"]}), lf.fill_null("one").collect
  end

  def test_map_batches
    lf = Polars::LazyFrame.new({"a" => [1, 2, 3]})
    lf2 = lf.map_batches { |x| x * 2 }
    GC.start
    assert_frame ({"a" => [2, 4, 6]}), lf2.collect
  end
end

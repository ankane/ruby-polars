require_relative "test_helper"

class DeltaTest < Minitest::Test
  def setup
    skip unless ENV["TEST_DELTA"]
    super
  end

  def test_read_delta
    df = Polars.read_delta("test/support/delta")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_delta_table
    df = Polars.read_delta(DeltaLake::Table.new("test/support/delta"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, df
  end

  def test_read_delta_columns
    df = Polars.read_delta("test/support/delta", columns: ["a"])
    expected = Polars::DataFrame.new({"a" => [1, 2, 3]})
    assert_frame expected, df
  end

  def test_read_delta_columns_order
    df = Polars.read_delta("test/support/delta", columns: ["b", "a"])
    assert_equal ["b", "a"], df.columns
  end

  def test_scan_delta
    lf = Polars.scan_delta("test/support/delta")
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, lf.collect
  end

  def test_scan_delta_table
    lf = Polars.scan_delta(DeltaLake::Table.new("test/support/delta"))
    expected = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    assert_frame expected, lf.collect
  end

  def test_write_delta
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_equal df, Polars.read_delta(path)
  end

  def test_write_delta_mode_error
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_raises(DeltaLake::Error) do
      df.write_delta(path, mode: "error")
    end
  end

  def test_write_delta_mode_append
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df2 = Polars::DataFrame.new({"a" => [4, 5, 6], "b" => ["four", "five", "six"]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_nil df2.write_delta(path, mode: "append")
    assert_equal Polars.concat([df, df2]), Polars.read_delta(path)
  end

  def test_write_delta_mode_overwrite
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df2 = Polars::DataFrame.new({"a" => [4, 5, 6], "b" => ["four", "five", "six"]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_nil df2.write_delta(path, mode: "overwrite")
    assert_equal df2, Polars.read_delta(path)
    assert_equal df2, Polars.read_delta(path, version: 1)
    assert_equal df2, Polars.scan_delta(path, version: 1).collect
    assert_equal df, Polars.read_delta(path, version: 0)
    assert_equal df, Polars.scan_delta(path, version: 0).collect
  end

  def test_write_delta_mode_ignore
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df2 = Polars::DataFrame.new({"a" => [4, 5, 6], "b" => ["four", "five", "six"]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_nil df2.write_delta(path, mode: "ignore")
    assert_equal df, Polars.read_delta(path)
  end

  def test_write_delta_mode_merge
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    df2 = Polars::DataFrame.new({"a" => [2, 3, 4], "b" => ["four", "five", "six"]})
    path = temp_path
    assert_nil df.write_delta(path)
    delta_merge_options = {
      predicate: "target.a = source.a",
      source_alias: "source",
      target_alias: "target"
    }
    df2.write_delta(path, mode: "merge", delta_merge_options: delta_merge_options)
      .when_matched_update({"a" => "source.a", "b" => "source.b"})
      .execute
    expected = Polars::DataFrame.new({"a" => [2, 3, 1], "b" => ["four", "five", "one"]})
    assert_equal expected, Polars.read_delta(path)
  end
end

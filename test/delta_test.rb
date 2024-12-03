require_relative "test_helper"

class DeltaTest < Minitest::Test
  def setup
    skip unless ENV["TEST_DELTA"]
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
end

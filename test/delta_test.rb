require_relative "test_helper"

class DeltaTest < Minitest::Test
  def setup
    skip unless ENV["TEST_DELTA"]
  end

  def test_read_delta
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_equal df, Polars.read_delta(path)
  end

  def test_read_delta_table
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_equal df, Polars.read_delta(DeltaLake::Table.new(path))
  end

  def test_scan_delta
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_equal df, Polars.scan_delta(path).collect
  end

  def test_scan_delta_table
    df = Polars::DataFrame.new({"a" => [1, 2, 3]})
    path = temp_path
    assert_nil df.write_delta(path)
    assert_equal df, Polars.scan_delta(DeltaLake::Table.new(path)).collect
  end
end

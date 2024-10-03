require_relative "test_helper"

class MarshalTest < Minitest::Test
  def test_series
    s = Polars::Series.new("a", [1, 2, 3])
    bin = Marshal.dump(s)
    assert_equal s, Marshal.load(bin)
  end

  def test_data_frame
    df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => ["one", "two", "three"]})
    bin = Marshal.dump(df)
    assert_equal df, Marshal.load(bin)
  end
end

require_relative "test_helper"

class LazyFrameTest < Minitest::Test
    def test_select
      df = Polars::DataFrame.new(
        {
          "foo" => [1, 2, 3],
          "bar" => [6, 7, 8],
          "ham" => ["a", "b", "c"],
        }
      ).lazy
      # p df.select("foo").collect
      # p df.select(["foo", "bar"]).collect
      p df.select(Polars.col("foo") + 1).collect
  end
end

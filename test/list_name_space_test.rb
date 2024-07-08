require_relative "test_helper"

class ListNameSpaceTest < Minitest::Test
  def test_get
    s = Polars::Series.new([[0, 1, 2], [0]])

    error = assert_raises(Polars::ComputeError) do
      s.list.get(1)
    end
    assert_equal "get index is out of bounds", error.message

    s.list.get(1, null_on_oob: true)
  end
end

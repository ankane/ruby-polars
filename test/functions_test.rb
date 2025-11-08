require_relative "test_helper"

class FunctionsTest < Minitest::Test
  def test_escape_regex
    assert_equal "\\^hello\\$", Polars.escape_regex("^hello$")
  end
end

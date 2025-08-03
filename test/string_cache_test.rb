require_relative "test_helper"

class StringCacheTest < Minitest::Test
  def test_works
    assert Polars.using_string_cache
    Polars::StringCache.new do
      assert Polars.using_string_cache
    end
    assert Polars.using_string_cache
  end

  def test_no_block
    error = assert_raises(LocalJumpError) do
      Polars::StringCache.new
    end
    assert_equal "no block given", error.message
  end

  def test_method
    Polars.string_cache do
      assert Polars.using_string_cache
    end
  end
end

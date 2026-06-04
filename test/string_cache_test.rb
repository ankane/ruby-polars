require_relative "test_helper"

class StringCacheTest < Minitest::Test
  def test_works
    assert Polars.using_string_cache
    Polars::StringCache.new do
      assert Polars.using_string_cache
    end
    assert Polars.using_string_cache
  end

  def test_method
    Polars.string_cache do
      assert Polars.using_string_cache
    end
  end
end

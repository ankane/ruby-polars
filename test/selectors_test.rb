require_relative "test_helper"

class SelectorsTest < Minitest::Test
  def test_inspect
    assert_inspect "Polars.cs.numeric()", Polars.cs.numeric
    assert_inspect "~Polars.cs.numeric()", ~Polars.cs.numeric
    assert_inspect "(Polars.cs.all() - Polars.cs.numeric())", Polars.cs.all - Polars.cs.numeric
    assert_inspect "(Polars.cs.float() & Polars.cs.integer())", Polars.cs.float & Polars.cs.integer
    assert_inspect "(Polars.cs.float() | Polars.cs.integer())", Polars.cs.float | Polars.cs.integer
  end

  def assert_inspect(expected, obj)
    assert_equal expected, obj.inspect
  end
end

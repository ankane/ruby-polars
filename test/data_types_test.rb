require_relative "test_helper"

class DataTypesTest < Minitest::Test
  def test_base_type
    assert_equal Polars::Int64, Polars::Int64.base_type
    assert_equal Polars::List, Polars::List.base_type
    assert_equal Polars::List, Polars::List.new(Polars::Int64).base_type
    assert_equal Polars::Duration, Polars::Duration.new("ns").base_type
  end

  def test_is_nested
    refute Polars::Int64.nested?
    assert Polars::List.nested?
    assert Polars::List.new(Polars::Int64).nested?
  end

  def test_to_s
    assert_equal "Polars::Int64", Polars::Int64.to_s
    assert_equal "Polars::List", Polars::List.to_s
    assert_equal "Polars::List(Polars::Int64)", Polars::List.new(Polars::Int64).to_s
  end
end

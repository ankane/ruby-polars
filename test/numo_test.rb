require_relative "test_helper"

class NumoTest < Minitest::Test
  def test_series_int
    s = Polars::Series.new([1, 2, 3])
    assert_kind_of Numo::Int64, s.to_numo
    assert_equal s.to_a, s.to_numo.to_a
  end

  def test_series_int_nil
    s = Polars::Series.new([1, nil, 3])
    assert_kind_of Numo::DFloat, s.to_numo
    assert s.to_numo[1].nan?
  end

  def test_series_float
    s = Polars::Series.new([1.5, 2.5, 3.5])
    assert_kind_of Numo::DFloat, s.to_numo
    assert_equal s.to_a, s.to_numo.to_a
  end

  def test_series_str
    s = Polars::Series.new(["one", "two", "three"])
    assert_kind_of Numo::RObject, s.to_numo
    assert_equal s.to_a, s.to_numo.to_a
  end
end

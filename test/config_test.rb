require_relative "test_helper"

class ConfigTest < Minitest::Test
  def test_set_tbl_rows
    s = Polars::Series.new(1..100)
    Polars::Config.new do |cfg|
      cfg.set_tbl_rows(100)
      refute_match "…", s.inspect
    end
    assert_match "…", s.inspect
  end
end

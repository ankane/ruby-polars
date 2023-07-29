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

  def test_state
    assert_empty Polars::Config.state(if_set: true, env_only: true)
  end
end

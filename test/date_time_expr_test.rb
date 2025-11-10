require_relative "test_helper"

class DateTimeExprTest < Minitest::Test
  def test_strftime
    assert_expr dt_expr.strftime("fmt")
  end

  def test_year
    assert_expr dt_expr.year
  end

  def test_iso_year
    assert_expr dt_expr.iso_year
  end

  def test_quarter
    assert_expr dt_expr.quarter
  end

  def test_month
    assert_expr dt_expr.month
  end

  def test_week
    assert_expr dt_expr.week
  end

  def test_weekday
    assert_expr dt_expr.weekday
  end

  def test_day
    assert_expr dt_expr.day
  end

  def test_ordinal_day
    assert_expr dt_expr.ordinal_day
  end

  def test_hour
    assert_expr dt_expr.hour
  end

  def test_minute
    assert_expr dt_expr.minute
  end

  def test_second
    assert_expr dt_expr.second
  end

  def test_millisecond
    assert_expr dt_expr.millisecond
  end

  def test_microsecond
    assert_expr dt_expr.microsecond
  end

  def test_nanosecond
    assert_expr dt_expr.nanosecond
  end

  def test_epoch
    assert_expr dt_expr.epoch
    assert_expr dt_expr.epoch("s")
    assert_expr dt_expr.epoch("d")
  end

  def test_timestamp
    assert_expr dt_expr.timestamp
  end

  def test_with_time_unit
    assert_expr dt_expr.with_time_unit("us")
  end

  def test_cast_time_unit
    assert_expr dt_expr.cast_time_unit("us")
  end

  def test_convert_time_zone
    assert_expr dt_expr.convert_time_zone("Etc/UTC")
  end

  def test_replace_time_zone
    assert_expr dt_expr.replace_time_zone("Etc/UTC")
  end

  def test_total_days
    assert_expr dt_expr.total_days
  end

  def test_total_hours
    assert_expr dt_expr.total_hours
  end

  def test_total_minutes
    assert_expr dt_expr.total_minutes
  end

  def test_total_seconds
    assert_expr dt_expr.total_seconds
  end

  def test_total_milliseconds
    assert_expr dt_expr.total_milliseconds
  end

  def test_total_microseconds
    assert_expr dt_expr.total_microseconds
  end

  def test_total_nanoseconds
    assert_expr dt_expr.total_nanoseconds
  end

  def test_offset_by
    assert_expr dt_expr.offset_by("1y")
  end

  def dt_expr
    Polars.col("a").dt
  end
end

require_relative "test_helper"

class DateTimeExprTest < Minitest::Test
  # def test_truncate
  # end

  # def test_round
  # end

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

  # def test_epoch
  # end

  # def test_timestamp
  # end

  # def test_with_time_unit
  # end

  # def test_cast_time_unit
  # end

  # def test_with_time_zone
  # end

  # def test_cast_time_zone
  # end

  # def test_tz_localize
  # end

  def test_days
    assert_expr dt_expr.days
  end

  def test_hours
    assert_expr dt_expr.hours
  end

  def test_minutes
    assert_expr dt_expr.minutes
  end

  def test_seconds
    assert_expr dt_expr.seconds
  end

  def test_milliseconds
    assert_expr dt_expr.milliseconds
  end

  def test_microseconds
    assert_expr dt_expr.microseconds
  end

  def test_nanoseconds
    assert_expr dt_expr.nanoseconds
  end

  # def test_offset_by
  # end

  def dt_expr
    Polars.col("a").dt
  end
end

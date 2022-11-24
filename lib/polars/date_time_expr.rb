module Polars
  class DateTimeExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # def truncate
    # end

    # def round
    # end

    def strftime(fmt)
      Utils.wrap_expr(_rbexpr.strftime(fmt))
    end

    def year
      Utils.wrap_expr(_rbexpr.year)
    end

    def iso_year
      Utils.wrap_expr(_rbexpr.iso_year)
    end

    def quarter
      Utils.wrap_expr(_rbexpr.quarter)
    end

    def month
      Utils.wrap_expr(_rbexpr.month)
    end

    def week
      Utils.wrap_expr(_rbexpr.week)
    end

    def weekday
      Utils.wrap_expr(_rbexpr.weekday)
    end

    def day
      Utils.wrap_expr(_rbexpr.day)
    end

    def ordinal_day
      Utils.wrap_expr(_rbexpr.ordinal_day)
    end

    def hour
      Utils.wrap_expr(_rbexpr.hour)
    end

    def minute
      Utils.wrap_expr(_rbexpr.minute)
    end

    def second
      Utils.wrap_expr(_rbexpr.second)
    end

    def millisecond
      Utils.wrap_expr(_rbexpr.millisecond)
    end

    def microsecond
      Utils.wrap_expr(_rbexpr.microsecond)
    end

    def nanosecond
      Utils.wrap_expr(_rbexpr.nanosecond)
    end

    # def epoch
    # end

    # def timestamp
    # end

    # def with_time_unit
    # end

    # def cast_time_unit
    # end

    # def with_time_zone
    # end

    # def cast_time_zone
    # end

    # def tz_localize
    # end

    def days
      Utils.wrap_expr(_rbexpr.duration_days)
    end

    def hours
      Utils.wrap_expr(_rbexpr.duration_hours)
    end

    def minutes
      Utils.wrap_expr(_rbexpr.duration_minutes)
    end

    def seconds
      Utils.wrap_expr(_rbexpr.duration_seconds)
    end

    def milliseconds
      Utils.wrap_expr(_rbexpr.duration_milliseconds)
    end

    def microseconds
      Utils.wrap_expr(_rbexpr.duration_microseconds)
    end

    def nanoseconds
      Utils.wrap_expr(_rbexpr.duration_nanoseconds)
    end

    # def offset_by
    # end
  end
end

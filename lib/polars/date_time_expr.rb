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

    # def strftime
    # end

    # def year
    # end

    # def iso_year
    # end

    # def quarter
    # end

    # def month
    # end

    # def week
    # end

    # def weekday
    # end

    # def day
    # end

    # def ordinal_day
    # end

    # def hour
    # end

    # def minute
    # end

    # def second
    # end

    # def millisecond
    # end

    # def microsecond
    # end

    # def nanosecond
    # end

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

    # def days
    # end

    # def hours
    # end

    # def minutes
    # end

    # def seconds
    # end

    # def milliseconds
    # end

    # def microseconds
    # end

    # def nanoseconds
    # end

    # def offset_by
    # end
  end
end

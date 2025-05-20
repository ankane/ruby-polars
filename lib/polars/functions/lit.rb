module Polars
  module Functions
    # Return an expression representing a literal value.
    #
    # @return [Expr]
    def lit(value, dtype: nil, allow_object: nil)
      if value.is_a?(::Time) || value.is_a?(::DateTime)
        time_unit = dtype&.time_unit || "ns"
        time_zone = dtype.&time_zone
        e = lit(Utils.datetime_to_int(value, time_unit)).cast(Datetime.new(time_unit))
        if time_zone
          return e.dt.replace_time_zone(time_zone.to_s)
        else
          return e
        end
      elsif value.is_a?(::Date)
        return lit(::Time.utc(value.year, value.month, value.day)).cast(Date)
      elsif value.is_a?(Polars::Series)
        value = value._s
        return Utils.wrap_expr(Plr.lit(value, allow_object, false))
      elsif (defined?(Numo::NArray) && value.is_a?(Numo::NArray)) || value.is_a?(::Array)
        return Utils.wrap_expr(Plr.lit(Series.new("literal", [value.to_a], dtype: dtype)._s, allow_object, true))
      elsif dtype
        return Utils.wrap_expr(Plr.lit(value, allow_object, true)).cast(dtype)
      end

      Utils.wrap_expr(Plr.lit(value, allow_object, true))
    end
  end
end

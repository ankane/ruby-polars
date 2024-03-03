module Polars
  module Functions
    # Return an expression representing a literal value.
    #
    # @return [Expr]
    def lit(value, dtype: nil, allow_object: nil)
      if value.is_a?(::Time) || value.is_a?(::DateTime)
        time_unit = dtype&.time_unit || "ns"
        time_zone = dtype.&time_zone
        e = lit(Utils._datetime_to_pl_timestamp(value, time_unit)).cast(Datetime.new(time_unit))
        if time_zone
          return e.dt.replace_time_zone(time_zone.to_s)
        else
          return e
        end
      elsif value.is_a?(::Date)
        return lit(::Time.utc(value.year, value.month, value.day)).cast(Date)
      elsif value.is_a?(Polars::Series)
        name = value.name
        value = value._s
        e = Utils.wrap_expr(Plr.lit(value, allow_object))
        if name == ""
          return e
        end
        return e.alias(name)
      elsif (defined?(Numo::NArray) && value.is_a?(Numo::NArray)) || value.is_a?(::Array)
        return lit(Series.new("", value))
      elsif dtype
        return Utils.wrap_expr(Plr.lit(value, allow_object)).cast(dtype)
      end

      Utils.wrap_expr(Plr.lit(value, allow_object))
    end
  end
end

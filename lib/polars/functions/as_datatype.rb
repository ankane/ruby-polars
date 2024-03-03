module Polars
  module Functions
    # Create polars `Duration` from distinct time components.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2)],
    #       "add" => [1, 2]
    #     }
    #   )
    #   df.select(
    #     [
    #       (Polars.col("datetime") + Polars.duration(weeks: "add")).alias("add_weeks"),
    #       (Polars.col("datetime") + Polars.duration(days: "add")).alias("add_days"),
    #       (Polars.col("datetime") + Polars.duration(seconds: "add")).alias("add_seconds"),
    #       (Polars.col("datetime") + Polars.duration(milliseconds: "add")).alias(
    #         "add_milliseconds"
    #       ),
    #       (Polars.col("datetime") + Polars.duration(hours: "add")).alias("add_hours")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 5)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬─────────────────────────┬─────────────────────┐
    #   # │ add_weeks           ┆ add_days            ┆ add_seconds         ┆ add_milliseconds        ┆ add_hours           │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---                     ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]            ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪═════════════════════════╪═════════════════════╡
    #   # │ 2022-01-08 00:00:00 ┆ 2022-01-02 00:00:00 ┆ 2022-01-01 00:00:01 ┆ 2022-01-01 00:00:00.001 ┆ 2022-01-01 01:00:00 │
    #   # │ 2022-01-16 00:00:00 ┆ 2022-01-04 00:00:00 ┆ 2022-01-02 00:00:02 ┆ 2022-01-02 00:00:00.002 ┆ 2022-01-02 02:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴─────────────────────────┴─────────────────────┘
    def duration(
      weeks: nil,
      days: nil,
      hours: nil,
      minutes: nil,
      seconds: nil,
      milliseconds: nil,
      microseconds: nil,
      nanoseconds: nil,
      time_unit: "us"
    )
      if !weeks.nil?
        weeks = Utils.expr_to_lit_or_expr(weeks, str_to_lit: false)._rbexpr
      end
      if !days.nil?
        days = Utils.expr_to_lit_or_expr(days, str_to_lit: false)._rbexpr
      end
      if !hours.nil?
        hours = Utils.expr_to_lit_or_expr(hours, str_to_lit: false)._rbexpr
      end
      if !minutes.nil?
        minutes = Utils.expr_to_lit_or_expr(minutes, str_to_lit: false)._rbexpr
      end
      if !seconds.nil?
        seconds = Utils.expr_to_lit_or_expr(seconds, str_to_lit: false)._rbexpr
      end
      if !milliseconds.nil?
        milliseconds = Utils.expr_to_lit_or_expr(milliseconds, str_to_lit: false)._rbexpr
      end
      if !microseconds.nil?
        microseconds = Utils.expr_to_lit_or_expr(microseconds, str_to_lit: false)._rbexpr
      end
      if !nanoseconds.nil?
        nanoseconds = Utils.expr_to_lit_or_expr(nanoseconds, str_to_lit: false)._rbexpr
      end

      Utils.wrap_expr(
        Plr.duration(
          weeks,
          days,
          hours,
          minutes,
          seconds,
          milliseconds,
          microseconds,
          nanoseconds,
          time_unit
        )
      )
    end
  end
end

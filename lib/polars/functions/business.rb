module Polars
  module Functions
    # Count the number of business days between `start` and `end` (not including `end`).
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param start [Object]
    #   Start dates.
    # @param stop [Object]
    #   End dates.
    # @param week_mask [Array]
    #   Which days of the week to count. The default is Monday to Friday.
    #   If you wanted to count only Monday to Thursday, you would pass
    #   `[true, true, true, true, false, false, false]`.
    # @param holidays [Array]
    #   Holidays to exclude from the count.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "start" => [Date.new(2020, 1, 1), Date.new(2020, 1, 2)],
    #       "end" => [Date.new(2020, 1, 2), Date.new(2020, 1, 10)]
    #     }
    #   )
    #   df.with_columns(
    #     business_day_count: Polars.business_day_count("start", "end")
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────────┬────────────┬────────────────────┐
    #   # │ start      ┆ end        ┆ business_day_count │
    #   # │ ---        ┆ ---        ┆ ---                │
    #   # │ date       ┆ date       ┆ i32                │
    #   # ╞════════════╪════════════╪════════════════════╡
    #   # │ 2020-01-01 ┆ 2020-01-02 ┆ 1                  │
    #   # │ 2020-01-02 ┆ 2020-01-10 ┆ 6                  │
    #   # └────────────┴────────────┴────────────────────┘
    #
    # @example You can pass a custom weekend - for example, if you only take Sunday off:
    #   week_mask = [true, true, true, true, true, true, false]
    #   df.with_columns(
    #     business_day_count: Polars.business_day_count(
    #       "start", "end", week_mask: week_mask
    #     )
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────────┬────────────┬────────────────────┐
    #   # │ start      ┆ end        ┆ business_day_count │
    #   # │ ---        ┆ ---        ┆ ---                │
    #   # │ date       ┆ date       ┆ i32                │
    #   # ╞════════════╪════════════╪════════════════════╡
    #   # │ 2020-01-01 ┆ 2020-01-02 ┆ 1                  │
    #   # │ 2020-01-02 ┆ 2020-01-10 ┆ 7                  │
    #   # └────────────┴────────────┴────────────────────┘
    #
    # @example You can also pass a list of holidays to exclude from the count:
    #   holidays = [Date.new(2020, 1, 1), Date.new(2020, 1, 2)]
    #   df.with_columns(
    #     business_day_count: Polars.business_day_count("start", "end", holidays: holidays)
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────────┬────────────┬────────────────────┐
    #   # │ start      ┆ end        ┆ business_day_count │
    #   # │ ---        ┆ ---        ┆ ---                │
    #   # │ date       ┆ date       ┆ i32                │
    #   # ╞════════════╪════════════╪════════════════════╡
    #   # │ 2020-01-01 ┆ 2020-01-02 ┆ 0                  │
    #   # │ 2020-01-02 ┆ 2020-01-10 ┆ 5                  │
    #   # └────────────┴────────────┴────────────────────┘
    def business_day_count(
      start,
      stop,
      week_mask: [true, true, true, true, true, false, false],
      holidays: []
    )
      start_rbexpr = Utils.parse_into_expression(start)
      end_rbexpr = Utils.parse_into_expression(stop)
      unix_epoch = ::Date.new(1970, 1, 1)
      Utils.wrap_expr(
        Plr.business_day_count(
          start_rbexpr,
          end_rbexpr,
          week_mask,
          holidays.map { |holiday| holiday - unix_epoch }
        )
      )
    end
  end
end

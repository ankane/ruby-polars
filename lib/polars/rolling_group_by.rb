module Polars
  # A rolling grouper.
  #
  # This has an `.agg` method which will allow you to run all polars expressions in a
  # group by context.
  class RollingGroupBy
    def initialize(
      df,
      index_column,
      period,
      offset,
      closed,
      by,
      check_sorted
    )
      period = Utils._timedelta_to_pl_duration(period)
      offset = Utils._timedelta_to_pl_duration(offset)

      @df = df
      @time_column = index_column
      @period = period
      @offset = offset
      @closed = closed
      @by = by
      @check_sorted = check_sorted
    end

    def agg(*aggs, **named_aggs)
      @df.lazy
        .group_by_rolling(
          index_column: @time_column, period: @period, offset: @offset, closed: @closed, by: @by, check_sorted: @check_sorted
        )
        .agg(*aggs, **named_aggs)
        .collect(no_optimization: true, string_cache: false)
    end
  end
end

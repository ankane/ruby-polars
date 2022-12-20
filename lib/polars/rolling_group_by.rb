module Polars
  # A rolling grouper.
  #
  # This has an `.agg` method which will allow you to run all polars expressions in a
  # groupby context.
  class RollingGroupBy
    def initialize(
      df,
      index_column,
      period,
      offset,
      closed,
      by
    )
      period = Utils._timedelta_to_pl_duration(period)
      offset = Utils._timedelta_to_pl_duration(offset)

      @df = df
      @time_column = index_column
      @period = period
      @offset = offset
      @closed = closed
      @by = by
    end

    def agg(aggs)
      @df.lazy
        .groupby_rolling(
          index_column: @time_column, period: @period, offset: @offset, closed: @closed, by: @by
        )
        .agg(aggs)
        .collect(no_optimization: true, string_cache: false)
    end
  end
end

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
      group_by
    )
      period = Utils.parse_as_duration_string(period)
      offset = Utils.parse_as_duration_string(offset)

      @df = df
      @time_column = index_column
      @period = period
      @offset = offset
      @closed = closed
      @group_by = group_by
    end

    def agg(*aggs, **named_aggs)
      @df.lazy
        .group_by_rolling(
          index_column: @time_column, period: @period, offset: @offset, closed: @closed, by: @group_by
        )
        .agg(*aggs, **named_aggs)
        .collect(no_optimization: true, string_cache: false)
    end
  end
end

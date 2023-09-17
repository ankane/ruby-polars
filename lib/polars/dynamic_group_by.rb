module Polars
  # A dynamic grouper.
  #
  # This has an `.agg` method which allows you to run all polars expressions in a
  # groupby context.
  class DynamicGroupBy
    def initialize(
      df,
      index_column,
      every,
      period,
      offset,
      truncate,
      include_boundaries,
      closed,
      by,
      start_by
    )
      period = Utils._timedelta_to_pl_duration(period)
      offset = Utils._timedelta_to_pl_duration(offset)
      every = Utils._timedelta_to_pl_duration(every)

      @df = df
      @time_column = index_column
      @every = every
      @period = period
      @offset = offset
      @truncate = truncate
      @include_boundaries = include_boundaries
      @closed = closed
      @by = by
      @start_by = start_by
    end

    def agg(aggs)
      @df.lazy
        .group_by_dynamic(
          @time_column,
          every: @every,
          period: @period,
          offset: @offset,
          truncate: @truncate,
          include_boundaries: @include_boundaries,
          closed: @closed,
          by: @by,
          start_by: @start_by
        )
        .agg(aggs)
        .collect(no_optimization: true, string_cache: false)
    end
  end
end

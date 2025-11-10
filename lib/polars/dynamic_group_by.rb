module Polars
  # A dynamic grouper.
  #
  # This has an `.agg` method which allows you to run all polars expressions in a
  # group by context.
  class DynamicGroupBy
    def initialize(
      df,
      index_column,
      every,
      period,
      offset,
      include_boundaries,
      closed,
      label,
      group_by,
      start_by
    )
      period = Utils.parse_as_duration_string(period)
      offset = Utils.parse_as_duration_string(offset)
      every = Utils.parse_as_duration_string(every)

      @df = df
      @time_column = index_column
      @every = every
      @period = period
      @offset = offset
      @include_boundaries = include_boundaries
      @closed = closed
      @label = label
      @group_by = group_by
      @start_by = start_by
    end

    def agg(*aggs, **named_aggs)
      @df.lazy
        .group_by_dynamic(
          @time_column,
          every: @every,
          period: @period,
          offset: @offset,
          include_boundaries: @include_boundaries,
          closed: @closed,
          label: @label,
          group_by: @group_by,
          start_by: @start_by
        )
        .agg(*aggs, **named_aggs)
        .collect(no_optimization: true, string_cache: false)
    end
  end
end

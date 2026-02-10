module Polars
  # A rolling grouper.
  #
  # This has an `.agg` method which will allow you to run all polars expressions in a
  # group by context.
  class RollingGroupBy
    # @private
    def initialize(
      df,
      index_column,
      period,
      offset,
      closed,
      group_by,
      predicates
    )
      period = Utils.parse_as_duration_string(period)
      offset = Utils.parse_as_duration_string(offset)

      @df = df
      @time_column = index_column
      @period = period
      @offset = offset
      @closed = closed
      @group_by = group_by
      @predicates = predicates
    end

    # Filter groups with a list of predicates after aggregation.
    #
    # Using this method is equivalent to adding the predicates to the aggregation and
    # filtering afterwards.
    #
    # This method can be chained and all conditions will be combined using `&`.
    #
    # @param predicates [Array]
    #   Expressions that evaluate to a boolean value for each group. Typically, this
    #   requires the use of an aggregation function. Multiple predicates are
    #   combined using `&`.
    #
    # @return [RollingGroupBy]
    def having(*predicates)
      RollingGroupBy.new(
        @df,
        @time_column,
        @period,
        @offset,
        @closed,
        @group_by,
        Utils._chain_predicates(@predicates, predicates)
      )
    end

    # Compute aggregations for each group of a group by operation.
    #
    # @param aggs [Array]
    #   Aggregations to compute for each group of the group by operation,
    #   specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names.
    # @param named_aggs [Hash]
    #   Additional aggregations, specified as keyword arguments.
    #   The resulting columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    def agg(*aggs, **named_aggs)
      group_by =
        @df.lazy.rolling(
          index_column: @time_column, period: @period, offset: @offset, closed: @closed, group_by: @group_by
        )

      if @predicates&.any?
        group_by = group_by.having(@predicates)
      end

      group_by.agg(*aggs, **named_aggs).collect(
        optimizations: QueryOptFlags.none
      )
    end
  end
end

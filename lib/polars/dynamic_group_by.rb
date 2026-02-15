module Polars
  # A dynamic grouper.
  #
  # This has an `.agg` method which allows you to run all polars expressions in a
  # group by context.
  class DynamicGroupBy
    # @private
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
      start_by,
      predicates
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
    # @return [DynamicGroupBy]
    def having(*predicates)
      DynamicGroupBy.new(
        @df,
        @time_column,
        @every,
        @period,
        @offset,
        @include_boundaries,
        @closed,
        @label,
        @group_by,
        @start_by,
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
        @df.lazy.group_by_dynamic(
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

      if @predicates&.any?
        group_by = group_by.having(@predicates)
      end

      group_by.agg(*aggs, **named_aggs).collect(
        optimizations: QueryOptFlags.none
      )
    end

    # Apply a custom/user-defined function (UDF) over the groups as a new DataFrame.
    #
    # Using this is considered an anti-pattern as it will be very slow because:
    #
    # - it forces the engine to materialize the whole `DataFrames` for the groups.
    # - it is not parallelized.
    # - it blocks optimizations as the passed python function is opaque to the
    #   optimizer.
    #
    # The idiomatic way to apply custom functions over multiple columns is using:
    #
    # `Polars.struct([my_columns]).map_elements { |struct_series| ... }`
    #
    # @param schema [Object]
    #   Schema of the output function. This has to be known statically. If the
    #   given schema is incorrect, this is a bug in the caller's query and may
    #   lead to errors. If set to None, polars assumes the schema is unchanged.
    #
    # @return [DataFrame]
    def map_groups(
      schema,
      &function
    )
      if @predicates&.any?
        msg = "cannot call `map_groups` when filtering groups with `having`"
        raise TypeError, msg
      end

      @df.lazy
        .group_by_dynamic(
          index_column: @time_column,
          every: @every,
          period: @period,
          offset: @offset,
          include_boundaries: @include_boundaries,
          closed: @closed,
          group_by: @group_by,
          start_by: @start_by
        )
        .map_groups(schema, &function)
        .collect(optimizations: QueryOptFlags.none)
    end
  end
end

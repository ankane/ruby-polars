module Polars
  # Starts a new GroupBy operation.
  class GroupBy
    # @private
    def initialize(df, *by, maintain_order:, predicates:, **named_by)
      @df = df
      @by = by
      @named_by = named_by
      @maintain_order = maintain_order
      @predicates = predicates
    end

    # Allows iteration over the groups of the group by operation.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["a", "a", "b"], "bar" => [1, 2, 3]})
    #   df.group_by("foo", maintain_order: true).each.to_h
    #   # =>
    #   # {["a"]=>shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 1   │
    #   # │ a   ┆ 2   │
    #   # └─────┴─────┘, ["b"]=>shape: (1, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ b   ┆ 3   │
    #   # └─────┴─────┘}
    def each
      return to_enum(:each) unless block_given?

      temp_col = "__POLARS_GB_GROUP_INDICES"
      groups_df =
        @df.lazy
          .with_row_index(name: temp_col)
          .group_by(@by, **@named_by, maintain_order: @maintain_order)
          .agg(Polars.col(temp_col))
          .collect(optimizations: QueryOptFlags.none)

      group_names = groups_df.select(Polars.all.exclude(temp_col))

      # When grouping by a single column, group name is a single value
      # When grouping by multiple columns, group name is a tuple of values
      if @by.is_a?(::String) || @by.is_a?(Expr)
        _group_names = group_names.to_series.each
      else
        _group_names = group_names.iter_rows
      end

      _group_indices = groups_df.select(temp_col).to_series
      _current_index = 0

      while _current_index < _group_indices.length
        group_name = _group_names.next
        group_data = @df[_group_indices[_current_index]]
        _current_index += 1

        yield group_name, group_data
      end
    end

    # Apply a custom/user-defined function (UDF) over the groups as a sub-DataFrame.
    #
    # Implementing logic using a Ruby function is almost always _significantly_
    # slower and more memory intensive than implementing the same logic using
    # the native expression API because:

    # - The native expression engine runs in Rust; UDFs run in Ruby.
    # - Use of Ruby UDFs forces the DataFrame to be materialized in memory.
    # - Polars-native expressions can be parallelised (UDFs cannot).
    # - Polars-native expressions can be logically optimised (UDFs cannot).
    #
    # Wherever possible you should strongly prefer the native expression API
    # to achieve the best performance.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "id" => [0, 1, 2, 3, 4],
    #       "color" => ["red", "green", "green", "red", "red"],
    #       "shape" => ["square", "triangle", "square", "triangle", "square"]
    #     }
    #   )
    #   df.group_by("color").apply { |group_df| group_df.sample(2) }
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬───────┬──────────┐
    #   # │ id  ┆ color ┆ shape    │
    #   # │ --- ┆ ---   ┆ ---      │
    #   # │ i64 ┆ str   ┆ str      │
    #   # ╞═════╪═══════╪══════════╡
    #   # │ 1   ┆ green ┆ triangle │
    #   # │ 2   ┆ green ┆ square   │
    #   # │ 4   ┆ red   ┆ square   │
    #   # │ 3   ┆ red   ┆ triangle │
    #   # └─────┴───────┴──────────┘
    # def apply(&f)
    #   _dataframe_class._from_rbdf(_df.group_by_apply(by, f))
    # end

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
    # @return [GroupBy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "c"]
    #     }
    #   )
    #   df.group_by("a").having(Polars.len > 1).agg
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ a   │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ b   │
    #   # │ a   │
    #   # └─────┘
    def having(*predicates)
      GroupBy.new(
        @df,
        *@by,
        maintain_order: @maintain_order,
        predicates: Utils._chain_predicates(@predicates, predicates),
        **@named_by
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
    #
    # @example Compute the aggregation of the columns for each group.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "c"],
    #       "b" => [1, 2, 1, 3, 3],
    #       "c" => [5, 4, 3, 2, 1]
    #     }
    #   )
    #   df.group_by("a").agg(Polars.col("b"), Polars.col("c"))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬───────────┬───────────┐
    #   # │ a   ┆ b         ┆ c         │
    #   # │ --- ┆ ---       ┆ ---       │
    #   # │ str ┆ list[i64] ┆ list[i64] │
    #   # ╞═════╪═══════════╪═══════════╡
    #   # │ a   ┆ [1, 1]    ┆ [5, 3]    │
    #   # │ b   ┆ [2, 3]    ┆ [4, 2]    │
    #   # │ c   ┆ [3]       ┆ [1]       │
    #   # └─────┴───────────┴───────────┘
    #
    # @example Compute the sum of a column for each group.
    #   df.group_by("a").agg(Polars.col("b").sum)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 2   │
    #   # │ b   ┆ 5   │
    #   # │ c   ┆ 3   │
    #   # └─────┴─────┘
    #
    # @example Compute multiple aggregates at once by passing a list of expressions.
    #   df.group_by("a").agg([Polars.sum("b"), Polars.mean("c")])
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ f64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ c   ┆ 3   ┆ 1.0 │
    #   # │ a   ┆ 2   ┆ 4.0 │
    #   # │ b   ┆ 5   ┆ 3.0 │
    #   # └─────┴─────┴─────┘
    #
    # @example Or use positional arguments to compute multiple aggregations in the same way.
    #   df.group_by("a").agg(
    #     Polars.sum("b").name.suffix("_sum"),
    #     (Polars.col("c") ** 2).mean.name.suffix("_mean_squared")
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬───────┬────────────────┐
    #   # │ a   ┆ b_sum ┆ c_mean_squared │
    #   # │ --- ┆ ---   ┆ ---            │
    #   # │ str ┆ i64   ┆ f64            │
    #   # ╞═════╪═══════╪════════════════╡
    #   # │ a   ┆ 2     ┆ 17.0           │
    #   # │ c   ┆ 3     ┆ 1.0            │
    #   # │ b   ┆ 5     ┆ 10.0           │
    #   # └─────┴───────┴────────────────┘
    #
    # @example Use keyword arguments to easily name your expression inputs.
    #   df.group_by("a").agg(
    #     b_sum: Polars.sum("b"),
    #     c_mean_squared: (Polars.col("c") ** 2).mean
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬───────┬────────────────┐
    #   # │ a   ┆ b_sum ┆ c_mean_squared │
    #   # │ --- ┆ ---   ┆ ---            │
    #   # │ str ┆ i64   ┆ f64            │
    #   # ╞═════╪═══════╪════════════════╡
    #   # │ a   ┆ 2     ┆ 17.0           │
    #   # │ c   ┆ 3     ┆ 1.0            │
    #   # │ b   ┆ 5     ┆ 10.0           │
    #   # └─────┴───────┴────────────────┘
    def agg(*aggs, **named_aggs)
      _lgb
        .agg(*aggs, **named_aggs)
        .collect(optimizations: QueryOptFlags.none)
    end

    # Apply a custom/user-defined function (UDF) over the groups as a sub-DataFrame.
    #
    # @note
    #   This method is much slower than the native expressions API.
    #   Only use it if you cannot implement your logic otherwise.
    #
    # Implementing logic using a Ruby function is almost always *significantly*
    # slower and more memory intensive than implementing the same logic using
    # the native expression API because:
    #
    # - The native expression engine runs in Rust; UDFs run in Ruby.
    # - Use of Ruby UDFs forces the DataFrame to be materialized in memory.
    # - Polars-native expressions can be parallelised (UDFs cannot).
    # - Polars-native expressions can be logically optimised (UDFs cannot).
    #
    # Wherever possible you should strongly prefer the native expression API
    # to achieve the best performance.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "id" => [0, 1, 2, 3, 4],
    #       "color" => ["red", "green", "green", "red", "red"],
    #       "shape" => ["square", "triangle", "square", "triangle", "square"]
    #     }
    #   )
    #   df.group_by("color").map_groups { |group_df| group_df.sample(n: 2) }
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬───────┬──────────┐
    #   # │ id  ┆ color ┆ shape    │
    #   # │ --- ┆ ---   ┆ ---      │
    #   # │ i64 ┆ str   ┆ str      │
    #   # ╞═════╪═══════╪══════════╡
    #   # │ 1   ┆ green ┆ triangle │
    #   # │ 2   ┆ green ┆ square   │
    #   # │ 4   ┆ red   ┆ square   │
    #   # │ 3   ┆ red   ┆ triangle │
    #   # └─────┴───────┴──────────┘
    def map_groups(&function)
      if @predicates&.any?
        msg = "cannot call `map_groups` when filtering groups with `having`"
        raise TypeError, msg
      end
      if @named_by&.any?
        msg = "cannot call `map_groups` when grouping by named expressions"
        raise TypeError, msg
      end
      if !@by.all? { |c| Utils.strlike?(c) }
        msg = "cannot call `map_groups` when grouping by an expression"
        raise TypeError, msg
      end

      by_strs = @by.map(&:to_s)

      @df.class._from_rbdf(
        @df._df.group_by_map_groups(by_strs, function, @maintain_order)
      )
    end

    # Get the first `n` rows of each group.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["c", "c", "a", "c", "a", "b"],
    #       "nrs" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   # =>
    #   # shape: (6, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ c       ┆ 1   │
    #   # │ c       ┆ 2   │
    #   # │ a       ┆ 3   │
    #   # │ c       ┆ 4   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # └─────────┴─────┘
    #
    # @example
    #   df.group_by("letters").head(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # │ c       ┆ 1   │
    #   # │ c       ┆ 2   │
    #   # └─────────┴─────┘
    def head(n = 5)
      _lgb.head(n).collect(optimizations: QueryOptFlags._eager)
    end

    # Get the last `n` rows of each group.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["c", "c", "a", "c", "a", "b"],
    #       "nrs" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   # =>
    #   # shape: (6, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ c       ┆ 1   │
    #   # │ c       ┆ 2   │
    #   # │ a       ┆ 3   │
    #   # │ c       ┆ 4   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # └─────────┴─────┘
    #
    # @example
    #   df.group_by("letters").tail(2).sort("letters")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────┬─────┐
    #   # │ letters ┆ nrs │
    #   # │ ---     ┆ --- │
    #   # │ str     ┆ i64 │
    #   # ╞═════════╪═════╡
    #   # │ a       ┆ 3   │
    #   # │ a       ┆ 5   │
    #   # │ b       ┆ 6   │
    #   # │ c       ┆ 2   │
    #   # │ c       ┆ 4   │
    #   # └─────────┴─────┘
    def tail(n = 5)
      _lgb.tail(n).collect(optimizations: QueryOptFlags._eager)
    end

    # Aggregate the groups into Series.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => ["one", "two", "one", "two"], "b" => [1, 2, 3, 4]})
    #   df.group_by("a", maintain_order: true).all
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬───────────┐
    #   # │ a   ┆ b         │
    #   # │ --- ┆ ---       │
    #   # │ str ┆ list[i64] │
    #   # ╞═════╪═══════════╡
    #   # │ one ┆ [1, 3]    │
    #   # │ two ┆ [2, 4]    │
    #   # └─────┴───────────┘
    def all
      agg(F.all)
    end

    # Return the number of rows in each group.
    #
    # @param name [String]
    #   Assign a name to the resulting column; if unset, defaults to "len".
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => ["Apple", "Apple", "Orange"], "b" => [1, nil, 2]})
    #   df.group_by("a").len
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────┬─────┐
    #   # │ a      ┆ len │
    #   # │ ---    ┆ --- │
    #   # │ str    ┆ u32 │
    #   # ╞════════╪═════╡
    #   # │ Apple  ┆ 2   │
    #   # │ Orange ┆ 1   │
    #   # └────────┴─────┘
    #
    # @example
    #   df.group_by("a").len(name: "n")
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────┬─────┐
    #   # │ a      ┆ n   │
    #   # │ ---    ┆ --- │
    #   # │ str    ┆ u32 │
    #   # ╞════════╪═════╡
    #   # │ Apple  ┆ 2   │
    #   # │ Orange ┆ 1   │
    #   # └────────┴─────┘
    def len(name: nil)
      len_expr = F.len
      if !name.nil?
        len_expr = len_expr.alias(name)
      end
      agg(len_expr)
    end

    # Aggregate the first values in the group.
    #
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default `false`).
    #   If set to `true`, the first non-null value for each aggregation is returned,
    #   otherwise `nil` is returned if no non-null value exists.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).first
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬───────┐
    #   # │ d      ┆ a   ┆ b    ┆ c     │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---   │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞════════╪═════╪══════╪═══════╡
    #   # │ Apple  ┆ 1   ┆ 0.5  ┆ true  │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true  │
    #   # │ Banana ┆ 4   ┆ 13.0 ┆ false │
    #   # └────────┴─────┴──────┴───────┘
    def first(ignore_nulls: false)
      agg(F.all.first(ignore_nulls: ignore_nulls))
    end

    # Aggregate the last values in the group.
    #
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default `false`).
    #   If set to `true`, the last non-null value for each aggregation is returned,
    #   otherwise `nil` is returned if no non-null value exists.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).last
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬───────┐
    #   # │ d      ┆ a   ┆ b    ┆ c     │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---   │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞════════╪═════╪══════╪═══════╡
    #   # │ Apple  ┆ 3   ┆ 10.0 ┆ false │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true  │
    #   # │ Banana ┆ 5   ┆ 14.0 ┆ true  │
    #   # └────────┴─────┴──────┴───────┘
    def last(ignore_nulls: false)
      agg(F.all.last(ignore_nulls: ignore_nulls))
    end

    # Reduce the groups to the sum.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).sum
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬─────┐
    #   # │ d      ┆ a   ┆ b    ┆ c   │
    #   # │ ---    ┆ --- ┆ ---  ┆ --- │
    #   # │ str    ┆ i64 ┆ f64  ┆ u32 │
    #   # ╞════════╪═════╪══════╪═════╡
    #   # │ Apple  ┆ 6   ┆ 14.5 ┆ 2   │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ 1   │
    #   # │ Banana ┆ 9   ┆ 27.0 ┆ 1   │
    #   # └────────┴─────┴──────┴─────┘
    def sum
      agg(Polars.all.sum)
    end

    # Reduce the groups to the minimal value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"],
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).min
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬───────┐
    #   # │ d      ┆ a   ┆ b    ┆ c     │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---   │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞════════╪═════╪══════╪═══════╡
    #   # │ Apple  ┆ 1   ┆ 0.5  ┆ false │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true  │
    #   # │ Banana ┆ 4   ┆ 13.0 ┆ false │
    #   # └────────┴─────┴──────┴───────┘
    def min
      agg(Polars.all.min)
    end

    # Reduce the groups to the maximal value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).max
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────┬──────┐
    #   # │ d      ┆ a   ┆ b    ┆ c    │
    #   # │ ---    ┆ --- ┆ ---  ┆ ---  │
    #   # │ str    ┆ i64 ┆ f64  ┆ bool │
    #   # ╞════════╪═════╪══════╪══════╡
    #   # │ Apple  ┆ 3   ┆ 10.0 ┆ true │
    #   # │ Orange ┆ 2   ┆ 0.5  ┆ true │
    #   # │ Banana ┆ 5   ┆ 14.0 ┆ true │
    #   # └────────┴─────┴──────┴──────┘
    def max
      agg(Polars.all.max)
    end

    # Count the number of values in each group.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).count
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬───────┐
    #   # │ d      ┆ count │
    #   # │ ---    ┆ ---   │
    #   # │ str    ┆ u32   │
    #   # ╞════════╪═══════╡
    #   # │ Apple  ┆ 3     │
    #   # │ Orange ┆ 1     │
    #   # │ Banana ┆ 2     │
    #   # └────────┴───────┘
    def count
      agg(Polars.len.alias("count"))
    end

    # Reduce the groups to the mean values.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "c" => [true, true, true, false, false, true],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).mean
    #   # =>
    #   # shape: (3, 4)
    #   # ┌────────┬─────┬──────────┬──────────┐
    #   # │ d      ┆ a   ┆ b        ┆ c        │
    #   # │ ---    ┆ --- ┆ ---      ┆ ---      │
    #   # │ str    ┆ f64 ┆ f64      ┆ f64      │
    #   # ╞════════╪═════╪══════════╪══════════╡
    #   # │ Apple  ┆ 2.0 ┆ 4.833333 ┆ 0.666667 │
    #   # │ Orange ┆ 2.0 ┆ 0.5      ┆ 1.0      │
    #   # │ Banana ┆ 4.5 ┆ 13.5     ┆ 0.5      │
    #   # └────────┴─────┴──────────┴──────────┘
    def mean
      agg(Polars.all.mean)
    end

    # Count the unique values per group.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 1, 3, 4, 5],
    #       "b" => [0.5, 0.5, 0.5, 10, 13, 14],
    #       "d" => ["Apple", "Banana", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).n_unique
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────┬─────┬─────┐
    #   # │ d      ┆ a   ┆ b   │
    #   # │ ---    ┆ --- ┆ --- │
    #   # │ str    ┆ u32 ┆ u32 │
    #   # ╞════════╪═════╪═════╡
    #   # │ Apple  ┆ 2   ┆ 2   │
    #   # │ Banana ┆ 3   ┆ 3   │
    #   # └────────┴─────┴─────┘
    def n_unique
      agg(Polars.all.n_unique)
    end

    # Compute the quantile per group.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "d" => ["Apple", "Orange", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).quantile(1)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────┬──────┐
    #   # │ d      ┆ a   ┆ b    │
    #   # │ ---    ┆ --- ┆ ---  │
    #   # │ str    ┆ f64 ┆ f64  │
    #   # ╞════════╪═════╪══════╡
    #   # │ Apple  ┆ 3.0 ┆ 10.0 │
    #   # │ Orange ┆ 2.0 ┆ 0.5  │
    #   # │ Banana ┆ 5.0 ┆ 14.0 │
    #   # └────────┴─────┴──────┘
    def quantile(quantile, interpolation: "nearest")
      agg(Polars.all.quantile(quantile, interpolation: interpolation))
    end

    # Return the median per group.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 4, 10, 13, 14],
    #       "d" => ["Apple", "Banana", "Apple", "Apple", "Banana", "Banana"]
    #     }
    #   )
    #   df.group_by("d", maintain_order: true).median
    #   # =>
    #   # shape: (2, 3)
    #   # ┌────────┬─────┬──────┐
    #   # │ d      ┆ a   ┆ b    │
    #   # │ ---    ┆ --- ┆ ---  │
    #   # │ str    ┆ f64 ┆ f64  │
    #   # ╞════════╪═════╪══════╡
    #   # │ Apple  ┆ 2.0 ┆ 4.0  │
    #   # │ Banana ┆ 4.0 ┆ 13.0 │
    #   # └────────┴─────┴──────┘
    def median
      agg(Polars.all.median)
    end

    private

    def _lgb
      group_by = @df.lazy.group_by(
        *@by, **@named_by, maintain_order: @maintain_order
      )
      if @predicates&.any?
        return group_by.having(@predicates)
      end
      group_by
    end
  end
end

module Polars
  # Starts a new GroupBy operation.
  class GroupBy
    # @private
    attr_accessor :_df, :_dataframe_class, :by, :maintain_order

    # @private
    def initialize(df, by, dataframe_class, maintain_order: false)
      self._df = df
      self._dataframe_class = dataframe_class
      self.by = by
      self.maintain_order = maintain_order
    end

    # Allows iteration over the groups of the groupby operation.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["a", "a", "b"], "bar" => [1, 2, 3]})
    #   df.groupby("foo", maintain_order: true).each.to_h
    #   # =>
    #   # {"a"=>shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 1   │
    #   # │ a   ┆ 2   │
    #   # └─────┴─────┘, "b"=>shape: (1, 2)
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
        Utils.wrap_df(_df)
          .lazy
          .with_row_count(name: temp_col)
          .group_by(by, maintain_order: maintain_order)
          .agg(Polars.col(temp_col))
          .collect(no_optimization: true)

      group_names = groups_df.select(Polars.all.exclude(temp_col))

      # When grouping by a single column, group name is a single value
      # When grouping by multiple columns, group name is a tuple of values
      if by.is_a?(String) || by.is_a?(Expr)
        _group_names = group_names.to_series.each
      else
        _group_names = group_names.iter_rows
      end

      _group_indices = groups_df.select(temp_col).to_series
      _current_index = 0

      while _current_index < _group_indices.length
        df = _dataframe_class._from_rbdf(_df)

        group_name = _group_names.next
        group_data = df[_group_indices[_current_index]]
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
    #   df.groupby("color").apply { |group_df| group_df.sample(2) }
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
    #   _dataframe_class._from_rbdf(_df.groupby_apply(by, f))
    # end

    # Use multiple aggregations on columns.
    #
    # This can be combined with complete lazy API and is considered idiomatic polars.
    #
    # @param aggs [Object]
    #   Single / multiple aggregation expression(s).
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => ["one", "two", "two", "one", "two"], "bar" => [5, 3, 2, 4, 1]}
    #   )
    #   df.groupby("foo", maintain_order: true).agg(
    #     [
    #       Polars.sum("bar").suffix("_sum"),
    #       Polars.col("bar").sort.tail(2).sum.suffix("_tail_sum")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────────┬──────────────┐
    #   # │ foo ┆ bar_sum ┆ bar_tail_sum │
    #   # │ --- ┆ ---     ┆ ---          │
    #   # │ str ┆ i64     ┆ i64          │
    #   # ╞═════╪═════════╪══════════════╡
    #   # │ one ┆ 9       ┆ 9            │
    #   # │ two ┆ 6       ┆ 5            │
    #   # └─────┴─────────┴──────────────┘
    def agg(aggs)
      df = Utils.wrap_df(_df)
        .lazy
        .group_by(by, maintain_order: maintain_order)
        .agg(aggs)
        .collect(no_optimization: true, string_cache: false)
      _dataframe_class._from_rbdf(df._df)
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
    #   df.groupby("letters").head(2).sort("letters")
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
      df = (
        Utils.wrap_df(_df)
          .lazy
          .groupby(by, maintain_order: maintain_order)
          .head(n)
          .collect(no_optimization: true, string_cache: false)
      )
      _dataframe_class._from_rbdf(df._df)
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
    #   df.groupby("letters").tail(2).sort("letters")
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
      df = (
        Utils.wrap_df(_df)
          .lazy
          .groupby(by, maintain_order: maintain_order)
          .tail(n)
          .collect(no_optimization: true, string_cache: false)
      )
      _dataframe_class._from_rbdf(df._df)
    end

    # Aggregate the first values in the group.
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
    #   df.groupby("d", maintain_order: true).first
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
    def first
      agg(Polars.all.first)
    end

    # Aggregate the last values in the group.
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
    #   df.groupby("d", maintain_order: true).last
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
    def last
      agg(Polars.all.last)
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
    #   df.groupby("d", maintain_order: true).sum
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
    #   df.groupby("d", maintain_order: true).min
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
    #   df.groupby("d", maintain_order: true).max
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
    #   df.groupby("d", maintain_order: true).count
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
      agg(Polars.count)
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
    #   df.groupby("d", maintain_order: true).mean
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
    #   df.groupby("d", maintain_order: true).n_unique
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
    #   df.groupby("d", maintain_order: true).quantile(1)
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
    #   df.groupby("d", maintain_order: true).median
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

    # Plot data.
    #
    # @return [Vega::LiteChart]
    def plot(*args, **options)
      raise ArgumentError, "Multiple groups not supported" if by.is_a?(::Array) && by.size > 1
      # same message as Ruby
      raise ArgumentError, "unknown keyword: :group" if options.key?(:group)

      Utils.wrap_df(_df).plot(*args, **options, group: by)
    end
  end
end

module Polars
  # A Series represents a single column in a polars DataFrame.
  class Series
    include ExprDispatch

    # Create a new Series.
    #
    # @param name [String, Array, nil]
    #   Name of the series. Will be used as a column name when used in a DataFrame.
    #   When not specified, name is set to an empty string.
    # @param values [Array, nil]
    #   One-dimensional data in various forms. Supported are: Array and Series.
    # @param dtype [Symbol, nil]
    #   Polars dtype of the Series data. If not specified, the dtype is inferred.
    # @param strict [Boolean]
    #   Throw error on numeric overflow.
    # @param nan_to_null [Boolean]
    #   Not used.
    # @param dtype_if_empty [Symbol, nil]
    #   If no dtype is specified and values contains `nil` or an empty array,
    #   set the Polars dtype of the Series data. If not specified, Float32 is used.
    #
    # @example Constructing a Series by specifying name and values positionally:
    #   s = Polars::Series.new("a", [1, 2, 3])
    #
    # @example Notice that the dtype is automatically inferred as a polars Int64:
    #   s.dtype
    #   # => :i64
    #
    # @example Constructing a Series with a specific dtype:
    #   s2 = Polars::Series.new("a", [1, 2, 3], dtype: :f32)
    #
    # @example It is possible to construct a Series with values as the first positional argument. This syntax considered an anti-pattern, but it can be useful in certain scenarios. You must specify any other arguments through keywords.
    #   s3 = Polars::Series.new([1, 2, 3])
    def initialize(name = nil, values = nil, dtype: nil, strict: true, nan_to_null: false, dtype_if_empty: nil)
      # Handle case where values are passed as the first argument
      if !name.nil? && !name.is_a?(String)
        if values.nil?
          values = name
          name = nil
        else
          raise ArgumentError, "Series name must be a string."
        end
      end

      name = "" if name.nil?

      if values.nil?
        self._s = sequence_to_rbseries(name, [], dtype: dtype, dtype_if_empty: dtype_if_empty)
      elsif values.is_a?(Series)
        self._s = series_to_rbseries(name, values)
      elsif values.is_a?(Range)
        self._s =
          Polars.arange(
            values.first,
            values.last + (values.exclude_end? ? 0 : 1),
            step: 1,
            eager: true,
            dtype: dtype
          )
          .rename(name, in_place: true)
          ._s
      elsif values.is_a?(Array)
        self._s = sequence_to_rbseries(name, values, dtype: dtype, strict: strict, dtype_if_empty: dtype_if_empty)
      else
        raise ArgumentError, "Series constructor called with unsupported type; got #{values.class.name}"
      end
    end

    # @private
    def self._from_rbseries(s)
      series = Series.allocate
      series._s = s
      series
    end

    # Get the data type of this Series.
    #
    # @return [Symbol]
    def dtype
      _s.dtype
    end

    # Get flags that are set on the Series.
    #
    # @return [Hash]
    def flags
      {
        "SORTED_ASC" => _s.is_sorted_flag,
        "SORTED_DESC" => _s.is_sorted_reverse_flag
      }
    end

    # Get the inner dtype in of a List typed Series.
    #
    # @return [Symbol]
    def inner_dtype
      _s.inner_dtype
    end

    # Get the name of this Series.
    #
    # @return [String]
    def name
      _s.name
    end

    # Shape of this Series.
    #
    # @return [Array]
    def shape
      [_s.len]
    end

    # Get the time unit of underlying Datetime Series as `"ns"`, `"us"`, or `"ms"`.
    #
    # @return [String]
    def time_unit
      _s.time_unit
    end

    # Returns a string representing the Series.
    #
    # @return [String]
    def to_s
      _s.to_s
    end
    alias_method :inspect, :to_s

    # Bitwise AND.
    #
    # @return [Series]
    def &(other)
      if !other.is_a?(Series)
        other = Series.new([other])
      end
      Utils.wrap_s(_s.bitand(other._s))
    end

    # Bitwise OR.
    #
    # @return [Series]
    def |(other)
      if !other.is_a?(Series)
        other = Series.new([other])
      end
      Utils.wrap_s(_s.bitor(other._s))
    end

    # Bitwise XOR.
    #
    # @return [Series]
    def ^(other)
      if !other.is_a?(Series)
        other = Series.new([other])
      end
      Utils.wrap_s(_s.bitxor(other._s))
    end

    # Equal.
    #
    # @return [Series]
    def ==(other)
      _comp(other, :eq)
    end

    # Not equal.
    #
    # @return [Series]
    def !=(other)
      _comp(other, :neq)
    end

    # Greater than.
    #
    # @return [Series]
    def >(other)
      _comp(other, :gt)
    end

    # Less than.
    #
    # @return [Series]
    def <(other)
      _comp(other, :lt)
    end

    # Greater than or equal.
    #
    # @return [Series]
    def >=(other)
      _comp(other, :gt_eq)
    end

    # Less than or equal.
    #
    # @return [Series]
    def <=(other)
      _comp(other, :lt_eq)
    end

    # Performs addition.
    #
    # @return [Series]
    def +(other)
      _arithmetic(other, :add)
    end

    # Performs subtraction.
    #
    # @return [Series]
    def -(other)
      _arithmetic(other, :sub)
    end

    # Performs multiplication.
    #
    # @return [Series]
    def *(other)
      _arithmetic(other, :mul)
    end

    # Performs division.
    #
    # @return [Series]
    def /(other)
      _arithmetic(other, :div)
    end

    # Returns the modulo.
    #
    # @return [Series]
    def %(other)
      if is_datelike
        raise ArgumentError, "first cast to integer before applying modulo on datelike dtypes"
      end
      _arithmetic(other, :rem)
    end

    # Raises to the power of exponent.
    #
    # @return [Series]
    def **(power)
      if is_datelike
        raise ArgumentError, "first cast to integer before raising datelike dtypes to a power"
      end
      to_frame.select(Polars.col(name).pow(power)).to_series
    end

    # Performs negation.
    #
    # @return [Series]
    def -@
      0 - self
    end

    # Returns elements of the Series.
    #
    # @return [Object]
    def [](item)
      if item.is_a?(Integer)
        return _s.get_idx(item)
      end

      if item.is_a?(Range)
        return Slice.new(self).apply(item)
      end

      raise ArgumentError, "Cannot get item of type: #{item.class.name}"
    end

    # Sets an element of the Series.
    #
    # @return [Object]
    def []=(key, value)
      if value.is_a?(Array)
        if is_numeric || is_datelike
          set_at_idx(key, value)
          return
        end
        raise ArgumentError, "cannot set Series of dtype: #{dtype} with list/tuple as value; use a scalar value"
      end

      if key.is_a?(Series)
        if key.dtype == :bool
          self._s = set(key, value)._s
        elsif key.dtype == :u64
          self._s = set_at_idx(key.cast(:u32), value)._s
        elsif key.dtype == :u32
          self._s = set_at_idx(key, value)._s
        else
          raise Todo
        end
      end

      if key.is_a?(Array)
        s = Utils.wrap_s(sequence_to_rbseries("", key, dtype: :u32))
        self[s] = value
      elsif key.is_a?(Integer)
        # TODO fix
        # self[[key]] = value
        set_at_idx(key, value)
      else
        raise ArgumentError, "cannot use #{key} for indexing"
      end
    end

    # Return an estimation of the total (heap) allocated size of the Series.
    #
    # Estimated size is given in the specified unit (bytes by default).
    #
    # This estimation is the sum of the size of its buffers, validity, including
    # nested arrays. Multiple arrays may share buffers and bitmaps. Therefore, the
    # size of 2 arrays is not the sum of the sizes computed from this function. In
    # particular, StructArray's size is an upper bound.
    #
    # When an array is sliced, its allocated size remains constant because the buffer
    # unchanged. However, this function will yield a smaller number. This is because
    # this function returns the visible size of the buffer, not its total capacity.
    #
    # FFI buffers are included in this estimation.
    #
    # @param unit ["b", "kb", "mb", "gb", "tb"]
    #   Scale the returned size to the given unit.
    #
    # @return [Numeric]
    #
    # @example
    #   s = Polars::Series.new("values", 1..1_000_000, dtype: :u32)
    #   s.estimated_size
    #   # => 4000000
    #   s.estimated_size("mb")
    #   # => 3.814697265625
    def estimated_size(unit = "b")
      sz = _s.estimated_size
      Utils.scale_bytes(sz, to: unit)
    end

    # Compute the square root of the elements.
    #
    # @return [Series]
    def sqrt
      self**0.5
    end

    # Check if any boolean value in the column is `true`.
    #
    # @return [Boolean]
    def any
      to_frame.select(Polars.col(name).any).to_series[0]
    end

    # Check if all boolean values in the column are `true`.
    #
    # @return [Boolean]
    def all
      to_frame.select(Polars.col(name).all).to_series[0]
    end

    # Compute the logarithm to a given base.
    #
    # @param base [Float]
    #   Given base, defaults to `Math::E`.
    #
    # @return [Series]
    def log(base = Math::E)
      super
    end

    # Compute the base 10 logarithm of the input array, element-wise.
    #
    # @return [Series]
    def log10
      super
    end

    # Compute the exponential, element-wise.
    #
    # @return [Series]
    def exp
      super
    end

    # Create a new Series that copies data from this Series without null values.
    #
    # @return [Series]
    def drop_nulls
      super
    end

    # Drop NaN values.
    #
    # @return [Series]
    def drop_nans
      super
    end

    # Cast this Series to a DataFrame.
    #
    # @return [DataFrame]
    def to_frame
      Utils.wrap_df(RbDataFrame.new([_s]))
    end

    # Quick summary statistics of a series.
    #
    # Series with mixed datatypes will return summary statistics for the datatype of
    # the first value.
    #
    # @return [DataFrame]
    #
    # @example
    #   series_num = Polars::Series.new([1, 2, 3, 4, 5])
    #   series_num.describe
    #   # =>
    #   # shape: (6, 2)
    #   # ┌────────────┬──────────┐
    #   # │ statistic  ┆ value    │
    #   # │ ---        ┆ ---      │
    #   # │ str        ┆ f64      │
    #   # ╞════════════╪══════════╡
    #   # │ min        ┆ 1.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ max        ┆ 5.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null_count ┆ 0.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ mean       ┆ 3.0      │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ std        ┆ 1.581139 │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
    #   # │ count      ┆ 5.0      │
    #   # └────────────┴──────────┘
    #
    # @example
    #   series_str = Polars::Series.new(["a", "a", nil, "b", "c"])
    #   series_str.describe
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────────┬───────┐
    #   # │ statistic  ┆ value │
    #   # │ ---        ┆ ---   │
    #   # │ str        ┆ i64   │
    #   # ╞════════════╪═══════╡
    #   # │ unique     ┆ 4     │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ null_count ┆ 1     │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ count      ┆ 5     │
    #   # └────────────┴───────┘
    def describe
      if len == 0
        raise ArgumentError, "Series must contain at least one value"
      elsif is_numeric
        s = cast(:f64)
        stats = {
          "min" => s.min,
          "max" => s.max,
          "null_count" => s.null_count,
          "mean" => s.mean,
          "std" => s.std,
          "count" => s.len
        }
      elsif is_boolean
        stats = {
          "sum" => sum,
          "null_count" => null_count,
          "count" => len
        }
      elsif is_utf8
        stats = {
          "unique" => unique.length,
          "null_count" => null_count,
          "count" => len
        }
      elsif is_datelike
        # we coerce all to string, because a polars column
        # only has a single dtype and dates: datetime and count: int don't match
        stats = {
          "min" => dt.min.to_s,
          "max" => dt.max.to_s,
          "null_count" => null_count.to_s,
          "count" => len.to_s
        }
      else
        raise TypeError, "This type is not supported"
      end

      Polars::DataFrame.new(
        {"statistic" => stats.keys, "value" => stats.values}
      )
    end

    # Reduce this Series to the sum value.
    #
    # @return [Numeric]
    #
    # @note
    #   Dtypes `:i8`, `:u8`, `:i16`, and `:u16` are cast to
    #   `:i64` before summing to prevent overflow issues.
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.sum
    #   # => 6
    def sum
      _s.sum
    end

    # Reduce this Series to the mean value.
    #
    # @return [Float, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.mean
    #   # => 2.0
    def mean
      _s.mean
    end

    # Reduce this Series to the product value.
    #
    # @return [Numeric]
    def product
      to_frame.select(Polars.col(name).product).to_series[0]
    end

    # Get the minimal value in this Series.
    #
    # @return [Object]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.min
    #   # => 1
    def min
      _s.min
    end

    # Get the maximum value in this Series.
    #
    # @return [Object]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.max
    #   # => 3
    def max
      _s.max
    end

    # Get maximum value, but propagate/poison encountered NaN values.
    #
    # @return [Object]
    def nan_max
      to_frame.select(Polars.col(name).nan_max)[0, 0]
    end

    # Get minimum value, but propagate/poison encountered NaN values.
    #
    # @return [Object]
    def nan_min
      to_frame.select(Polars.col(name).nan_min)[0, 0]
    end

    # Get the standard deviation of this Series.
    #
    # @param ddof [Integer]
    #  “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #
    # @return [Float, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.std
    #   # => 1.0
    def std(ddof: 1)
      if !is_numeric
        nil
      else
        to_frame.select(Polars.col(name).std(ddof: ddof)).to_series[0]
      end
    end

    # Get variance of this Series.
    #
    # @param ddof [Integer]
    #  “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof,
    #   where N represents the number of elements.
    #
    # @return [Float, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.var
    #   # => 1.0
    def var(ddof: 1)
      if !is_numeric
        nil
      else
        to_frame.select(Polars.col(name).var(ddof: ddof)).to_series[0]
      end
    end

    # Get the median of this Series.
    #
    # @return [Float, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.median
    #   # => 2.0
    def median
      _s.median
    end

    # Get the quantile value of this Series.
    #
    # @param quantile [Float, nil]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    #
    # @return [Float, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.quantile(0.5)
    #   # => 2.0
    def quantile(quantile, interpolation: "nearest")
      _s.quantile(quantile, interpolation)
    end

    # Get dummy variables.
    #
    # @return [DataFrame]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.to_dummies
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a_1 ┆ a_2 ┆ a_3 │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ u8  ┆ u8  ┆ u8  │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 0   ┆ 0   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 0   ┆ 1   ┆ 0   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 0   ┆ 0   ┆ 1   │
    #   # └─────┴─────┴─────┘
    def to_dummies
      Utils.wrap_df(_s.to_dummies)
    end

    # Count the unique values in a Series.
    #
    # @param sort [Boolean]
    #   Ensure the output is sorted from most values to least.
    #
    # @return [DataFrame]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.value_counts.sort("a")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬────────┐
    #   # │ a   ┆ counts │
    #   # │ --- ┆ ---    │
    #   # │ i64 ┆ u32    │
    #   # ╞═════╪════════╡
    #   # │ 1   ┆ 1      │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 2      │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 1      │
    #   # └─────┴────────┘
    def value_counts(sort: false)
      Utils.wrap_df(_s.value_counts(sort))
    end

    # Return a count of the unique values in the order of appearance.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("id", ["a", "b", "b", "c", "c", "c"])
    #   s.unique_counts
    #   # =>
    #   # shape: (3,)
    #   # Series: 'id' [u32]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   # ]
    def unique_counts
      super
    end

    # Computes the entropy.
    #
    # Uses the formula `-sum(pk * log(pk)` where `pk` are discrete probabilities.
    #
    # @param base [Float]
    #   Given base, defaults to `e`
    # @param normalize [Boolean]
    #   Normalize pk if it doesn't sum to 1.
    #
    # @return [Float, nil]
    #
    # @example
    #   a = Polars::Series.new([0.99, 0.005, 0.005])
    #   a.entropy(normalize: true)
    #   # => 0.06293300616044681
    #
    # @example
    #   b = Polars::Series.new([0.65, 0.10, 0.25])
    #   b.entropy(normalize: true)
    #   # => 0.8568409950394724
    def entropy(base: Math::E, normalize: false)
      Polars.select(Polars.lit(self).entropy(base: base, normalize: normalize)).to_series[0]
    end

    # Run an expression over a sliding window that increases `1` slot every iteration.
    #
    # @param expr [Expr]
    #   Expression to evaluate
    # @param min_periods [Integer]
    #   Number of valid values there should be in the window before the expression
    #   is evaluated. valid values = `length - null_count`
    # @param parallel [Boolean]
    #   Run in parallel. Don't do this in a groupby or another operation that
    #   already has much parallelization.
    #
    # @return [Series]
    #
    # @note
    #   This functionality is experimental and may change without it being considered a
    #   breaking change.
    #
    # @note
    #   This can be really slow as it can have `O(n^2)` complexity. Don't use this
    #   for operations that visit all elements.
    #
    # @example
    #   s = Polars::Series.new("values", [1, 2, 3, 4, 5])
    #   s.cumulative_eval(Polars.element.first - Polars.element.last ** 2)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'values' [f64]
    #   # [
    #   #         0.0
    #   #         -3.0
    #   #         -8.0
    #   #         -15.0
    #   #         -24.0
    #   # ]
    def cumulative_eval(expr, min_periods: 1, parallel: false)
      super
    end

    # Return a copy of the Series with a new alias/name.
    #
    # @param name [String]
    #   New name.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("x", [1, 2, 3])
    #   s.alias("y")
    def alias(name)
      s = dup
      s._s.rename(name)
      s
    end

    # Rename this Series.
    #
    # @param name [String]
    #   New name.
    # @param in_place [Boolean]
    #   Modify the Series in-place.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.rename("b")
    def rename(name, in_place: false)
      if in_place
        _s.rename(name)
        self
      else
        self.alias(name)
      end
    end

    # Get the length of each individual chunk.
    #
    # @return [Array]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s2 = Polars::Series.new("b", [4, 5, 6])
    #
    # @example Concatenate Series with rechunk: true
    #   Polars.concat([s, s2]).chunk_lengths
    #   # => [6]
    #
    # @example Concatenate Series with rechunk: false
    #   Polars.concat([s, s2], rechunk: false).chunk_lengths
    #   # => [3, 3]
    def chunk_lengths
      _s.chunk_lengths
    end

    # Get the number of chunks that this Series contains.
    #
    # @return [Integer]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s2 = Polars::Series.new("b", [4, 5, 6])
    #
    # @example Concatenate Series with rechunk: true
    #   Polars.concat([s, s2]).n_chunks
    #   # => 1
    #
    # @example Concatenate Series with rechunk: false
    #   Polars.concat([s, s2], rechunk: false).n_chunks
    #   # => 2
    def n_chunks
      _s.n_chunks
    end

    # Get an array with the cumulative sum computed at every element.
    #
    # @param reverse [Boolean]
    #   reverse the operation.
    #
    # @return [Series]
    #
    # @note
    #   Dtypes `:i8`, `:u8`, `:i16`, and `:u16` are cast to
    #   `:i64` before summing to prevent overflow issues.
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.cumsum
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         3
    #   #         6
    #   # ]
    def cumsum(reverse: false)
      super
    end

    # Get an array with the cumulative min computed at every element.
    #
    # @param reverse [Boolean]
    #   reverse the operation.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [3, 5, 1])
    #   s.cummin
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         3
    #   #         3
    #   #         1
    #   # ]
    def cummin(reverse: false)
      super
    end

    # Get an array with the cumulative max computed at every element.
    #
    # @param reverse [Boolean]
    #   reverse the operation.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [3, 5, 1])
    #   s.cummax
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         3
    #   #         5
    #   #         5
    #   # ]
    def cummax(reverse: false)
      super
    end

    # Get an array with the cumulative product computed at every element.
    #
    # @param reverse [Boolean]
    #   reverse the operation.
    #
    # @return [Series]
    #
    # @note
    #   Dtypes `:i8`, `:u8`, `:i16`, and `:u16` are cast to
    #   `:i64` before multiplying to prevent overflow issues.
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.cumprod
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         6
    #   # ]
    def cumprod(reverse: false)
      super
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.limit(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   # ]
    def limit(n = 10)
      to_frame.select(Utils.col(name).limit(n)).to_series
    end

    # Get a slice of this Series.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer, nil]
    #   Length of the slice. If set to `nil`, all rows starting at the offset
    #   will be selected.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, 4])
    #   s.slice(1, 2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   #         3
    #   # ]
    def slice(offset, length = nil)
      super
    end

    # Append a Series to this one.
    #
    # @param other [Series]
    #   Series to append.
    # @param append_chunks [Boolean]
    #   If set to `true` the append operation will add the chunks from `other` to
    #   self. This is super cheap.
    #
    #   If set to `false` the append operation will do the same as
    #   {DataFrame#extend} which extends the memory backed by this Series with
    #   the values from `other`.
    #
    #   Different from `append_chunks`, `extend` appends the data from `other` to
    #   the underlying memory locations and thus may cause a reallocation (which is
    #   expensive).
    #
    #   If this does not cause a reallocation, the resulting data structure will not
    #   have any extra chunks and thus will yield faster queries.
    #
    #   Prefer `extend` over `append_chunks` when you want to do a query after a
    #   single append. For instance during online operations where you add `n` rows
    #   and rerun a query.
    #
    #   Prefer `append_chunks` over `extend` when you want to append many times
    #   before doing a query. For instance, when you read in multiple files and when
    #   to store them in a single Series. In the latter case, finish the sequence
    #   of `append_chunks` operations with a `rechunk`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s2 = Polars::Series.new("b", [4, 5, 6])
    #   s.append(s2)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   #         5
    #   #         6
    #   # ]
    def append(other, append_chunks: true)
      begin
        if append_chunks
          _s.append(other._s)
        else
          _s.extend(other._s)
        end
      rescue => e
        if e.message == "Already mutably borrowed"
          append(other.clone, append_chunks)
        else
          raise e
        end
      end
      self
    end

    # Filter elements by a boolean mask.
    #
    # @param predicate [Series, Array]
    #   Boolean mask.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   mask = Polars::Series.new("", [true, false, true])
    #   s.filter(mask)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def filter(predicate)
      if predicate.is_a?(Array)
        predicate = Series.new("", predicate)
      end
      Utils.wrap_s(_s.filter(predicate._s))
    end

    # Get the first `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.head(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   # ]
    def head(n = 10)
      to_frame.select(Utils.col(name).head(n)).to_series
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.tail(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   #         3
    #   # ]
    def tail(n = 10)
      to_frame.select(Utils.col(name).tail(n)).to_series
    end

    # Take every nth value in the Series and return as new Series.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, 4])
    #   s.take_every(2)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def take_every(n)
      super
    end

    # Sort this Series.
    #
    # @param reverse [Boolean]
    #   Reverse sort.
    # @param in_place [Boolean]
    #   Sort in place.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 3, 4, 2])
    #   s.sort
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   # ]
    #   s.sort(reverse: true)
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         4
    #   #         3
    #   #         2
    #   #         1
    #   # ]
    def sort(reverse: false, in_place: false)
      if in_place
        self._s = _s.sort(reverse)
        self
      else
        Utils.wrap_s(_s.sort(reverse))
      end
    end

    # Return the `k` largest elements.
    #
    # If `reverse: true`, the smallest elements will be given.
    #
    # @param k [Integer]
    #   Number of elements to return.
    # @param reverse [Boolean]
    #   Return the smallest elements.
    #
    # @return [Boolean]
    def top_k(k: 5, reverse: false)
      super
    end

    # Get the index values that would sort this Series.
    #
    # @param reverse [Boolean]
    #   Sort in reverse (descending) order.
    # @param nulls_last [Boolean]
    #   Place null values last instead of first.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [5, 3, 4, 1, 2])
    #   s.arg_sort
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         3
    #   #         4
    #   #         1
    #   #         2
    #   #         0
    #   # ]
    def arg_sort(reverse: false, nulls_last: false)
      super
    end

    # Get the index values that would sort this Series.
    #
    # Alias for {#arg_sort}.
    #
    # @param reverse [Boolean]
    #   Sort in reverse (descending) order.
    # @param nulls_last [Boolean]
    #   Place null values last instead of first.
    #
    # @return [Series]
    def argsort(reverse: false, nulls_last: false)
      super
    end

    # Get unique index as Series.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.arg_unique
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         0
    #   #         1
    #   #         3
    #   # ]
    def arg_unique
      super
    end

    # Get the index of the minimal value.
    #
    # @return [Integer, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [3, 2, 1])
    #   s.arg_min
    #   # => 2
    def arg_min
      _s.arg_min
    end

    # Get the index of the maximal value.
    #
    # @return [Integer, nil]
    #
    # @example
    #   s = Polars::Series.new("a", [3, 2, 1])
    #   s.arg_max
    #   # => 0
    def arg_max
      _s.arg_max
    end

    # Find indices where elements should be inserted to maintain order.
    #
    # @param element [Object]
    #   Expression or scalar value.
    #
    # @return [Integer]
    def search_sorted(element)
      Polars.select(Polars.lit(self).search_sorted(element))[0, 0]
    end

    # Get unique elements in series.
    #
    # @param maintain_order [Boolean]
    #   Maintain order of data. This requires more work.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.unique.sort
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   # ]
    def unique(maintain_order: false)
      super
    end

    # Take values by index.
    #
    # @param indices [Array]
    #   Index location used for selection.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, 4])
    #   s.take([1, 3])
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   #         4
    #   # ]
    def take(indices)
      to_frame.select(Polars.col(name).take(indices)).to_series
    end

    # Count the null values in this Series.
    #
    # @return [Integer]
    def null_count
      _s.null_count
    end

    # Return `true` if the Series has a validity bitmask.
    #
    # If there is none, it means that there are no null values.
    # Use this to swiftly assert a Series does not have null values.
    #
    # @return [Boolean]
    def has_validity
      _s.has_validity
    end

    # Check if the Series is empty.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new("a", [])
    #   s.is_empty
    #   # => true
    def is_empty
      len == 0
    end
    alias_method :empty?, :is_empty

    # Returns a boolean Series indicating which values are null.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, nil])
    #   s.is_null
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         false
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def is_null
      super
    end

    # Returns a boolean Series indicating which values are not null.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, nil])
    #   s.is_not_null
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_not_null
      super
    end

    # Returns a boolean Series indicating which values are finite.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, Float::INFINITY])
    #   s.is_finite
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_finite
      super
    end

    # Returns a boolean Series indicating which values are infinite.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, Float::INFINITY])
    #   s.is_infinite
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def is_infinite
      super
    end

    # Returns a boolean Series indicating which values are NaN.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, Float::NAN])
    #   s.is_nan
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         false
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def is_nan
      super
    end

    # Returns a boolean Series indicating which values are not NaN.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, Float::NAN])
    #   s.is_not_nan
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_not_nan
      super
    end

    # Check if elements of this Series are in the other Series.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s2 = Polars::Series.new("b", [2, 4])
    #   s2.is_in(s)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'b' [bool]
    #   # [
    #   #         true
    #   #         false
    #   # ]
    #
    # @example
    #   sets = Polars::Series.new("sets", [[1, 2, 3], [1, 2], [9, 10]])
    #   # =>
    #   # shape: (3,)
    #   # Series: 'sets' [list]
    #   # [
    #   #         [1, 2, 3]
    #   #         [1, 2]
    #   #         [9, 10]
    #   # ]
    #
    # @example
    #   optional_members = Polars::Series.new("optional_members", [1, 2, 3])
    #   # =>
    #   # shape: (3,)
    #   # Series: 'optional_members' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   # ]
    #
    # @example
    #   optional_members.is_in(sets)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'optional_members' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_in(other)
      super
    end

    # Get index values where Boolean Series evaluate `true`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   (s == 2).arg_true
    #   # =>
    #   # shape: (1,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   # ]
    def arg_true
      Polars.arg_where(self, eager: true)
    end

    # Get mask of all unique values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.is_unique
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def is_unique
      super
    end

    # Get a mask of the first unique value.
    #
    # @return [Series]
    def is_first
      super
    end

    # Get mask of all duplicated values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.is_duplicated
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_duplicated
      super
    end

    # Explode a list or utf8 Series.
    #
    # This means that every item is expanded to a new row.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [[1, 2], [3, 4], [9, 10]])
    #   s.explode
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   #         9
    #   #         10
    #   # ]
    def explode
      super
    end

    # Check if series is equal with another Series.
    #
    # @param other [Series]
    #   Series to compare with.
    # @param null_equal [Boolean]
    #   Consider null values as equal.
    # @param strict [Boolean]
    #   Don't allow different numerical dtypes, e.g. comparing `:u32` with a
    #   `:i64` will return `false`.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s2 = Polars::Series.new("b", [4, 5, 6])
    #   s.series_equal(s)
    #   # => true
    #   s.series_equal(s2)
    #   # => false
    def series_equal(other, null_equal: false, strict: false)
      _s.series_equal(other._s, null_equal, strict)
    end

    # Length of this Series.
    #
    # @return [Integer]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.len
    #   # => 3
    def len
      _s.len
    end
    alias_method :length, :len

    # Cast between data types.
    #
    # @param dtype [Symbol]
    #   DataType to cast to
    # @param strict [Boolean]
    #   Throw an error if a cast could not be done for instance due to an overflow
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [true, false, true])
    #   s.cast(:u32)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         1
    #   #         0
    #   #         1
    #   # ]
    def cast(dtype, strict: true)
      super
    end

    # Cast to physical representation of the logical dtype.
    #
    # - `:date` -> `:i32`
    # - `:datetime` -> `:i64`
    # - `:time` -> `:i64`
    # - `:duration` -> `:i64`
    # - `:cat` -> `:u32`
    # - other data types will be left unchanged.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("values", ["a", nil, "x", "a"])
    #   s.cast(:cat).to_physical
    #   # =>
    #   # shape: (4,)
    #   # Series: 'values' [u32]
    #   # [
    #   #         0
    #   #         null
    #   #         1
    #   #         0
    #   # ]
    def to_physical
      super
    end

    # Convert this Series to a Ruby Array. This operation clones data.
    #
    # @return [Array]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.to_a
    #   # => [1, 2, 3]
    def to_a
      _s.to_a
    end

    # Create a single chunk of memory for this Series.
    #
    # @param in_place [Boolean]
    #   In place or not.
    #
    # @return [Series]
    def rechunk(in_place: false)
      opt_s = _s.rechunk(in_place)
      in_place ? self : Utils.wrap_s(opt_s)
    end

    # Return Series in reverse order.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3], dtype: :i8)
    #   s.reverse
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i8]
    #   # [
    #   #         3
    #   #         2
    #   #         1
    #   # ]
    def reverse
      super
    end

    # Check if this Series datatype is numeric.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.is_numeric
    #   # => true
    def is_numeric
      [:i8, :i16, :i32, :i64, :u8, :u16, :u32, :u64, :f32, :f64].include?(dtype)
    end
    alias_method :numeric?, :is_numeric

    # Check if this Series datatype is datelike.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new([Date.new(2021, 1, 1), Date.new(2021, 1, 2), Date.new(2021, 1, 3)])
    #   s.is_datelike
    #   # => true
    def is_datelike
      [:date, :datetime, :duration, :time].include?(dtype)
    end

    # Check if this Series has floating point numbers.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0])
    #   s.is_float
    #   # => true
    def is_float
      [:f32, :f64].include?(dtype)
    end
    alias_method :float?, :is_float

    # Check if this Series is a Boolean.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new("a", [true, false, true])
    #   s.is_boolean
    #   # => true
    def is_boolean
      dtype == :bool
    end
    alias_method :boolean?, :is_boolean
    alias_method :is_bool, :is_boolean
    alias_method :bool?, :is_boolean

    # Check if this Series datatype is a Utf8.
    #
    # @return [Boolean]
    #
    # @example
    #   s = Polars::Series.new("x", ["a", "b", "c"])
    #   s.is_utf8
    #   # => true
    def is_utf8
      dtype == :str
    end
    alias_method :utf8?, :is_utf8

    # def view
    # end

    # def to_numo
    # end

    # def set
    # end

    # Set values at the index locations.
    #
    # @param idx [Object]
    #   Integers representing the index locations.
    # @param value [Object]
    #   Replacement values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.set_at_idx(1, 10)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         10
    #   #         3
    #   # ]
    def set_at_idx(idx, value)
      if idx.is_a?(Integer)
        idx = [idx]
      end
      if idx.length == 0
        return self
      end

      idx = Series.new("", idx)
      if value.is_a?(Integer) || value.is_a?(Float) || Utils.bool?(value) || value.is_a?(String) || value.nil?
        value = Series.new("", [value])

        # if we need to set more than a single value, we extend it
        if idx.length > 0
          value = value.extend_constant(value[0], idx.length - 1)
        end
      elsif !value.is_a?(Series)
        value = Series.new("", value)
      end
      _s.set_at_idx(idx._s, value._s)
      self
    end

    # Create an empty copy of the current Series.
    #
    # The copy has identical name/dtype but no data.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [nil, true, false])
    #   s.cleared
    #   # =>
    #   # shape: (0,)
    #   # Series: 'a' [bool]
    #   # [
    #   # ]
    def cleared
      len > 0 ? limit(0) : clone
    end

    # clone handled by initialize_copy

    # Fill floating point NaN value with a fill value.
    #
    # @param fill_value [Object]
    #   Value used to fill nan values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, Float::NAN])
    #   s.fill_nan(0)
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.0
    #   #         2.0
    #   #         3.0
    #   #         0.0
    #   # ]
    def fill_nan(fill_value)
      super
    end

    # Fill null values using the specified value or strategy.
    #
    # @param value [Object]
    #   Value used to fill null values.
    # @param strategy [nil, "forward", "backward", "min", "max", "mean", "zero", "one"]
    #   Strategy used to fill null values.
    # @param limit
    #   Number of consecutive null values to fill when using the "forward" or
    #   "backward" strategy.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, nil])
    #   s.fill_null(strategy: "forward")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         3
    #   # ]
    #
    # @example
    #   s.fill_null(strategy: "min")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         1
    #   # ]
    #
    # @example
    #   s = Polars::Series.new("b", ["x", nil, "z"])
    #   s.fill_null(Polars.lit(""))
    #   # =>
    #   # shape: (3,)
    #   # Series: 'b' [str]
    #   # [
    #   #         "x"
    #   #         ""
    #   #         "z"
    #   # ]
    def fill_null(value = nil, strategy: nil, limit: nil)
      super
    end

    # Rounds down to the nearest integer value.
    #
    # Only works on floating point Series.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.12345, 2.56789, 3.901234])
    #   s.floor
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.0
    #   #         2.0
    #   #         3.0
    #   # ]
    def floor
      Utils.wrap_s(_s.floor)
    end

    # Rounds up to the nearest integer value.
    #
    # Only works on floating point Series.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.12345, 2.56789, 3.901234])
    #   s.ceil
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         2.0
    #   #         3.0
    #   #         4.0
    #   # ]
    def ceil
      super
    end

    # Round underlying floating point data by `decimals` digits.
    #
    # @param decimals [Integer]
    #   number of decimals to round by.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.12345, 2.56789, 3.901234])
    #   s.round(2)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.12
    #   #         2.57
    #   #         3.9
    #   # ]
    def round(decimals = 0)
      super
    end

    # Compute the dot/inner product between two Series.
    #
    # @param other [Object]
    #   Series (or array) to compute dot product with.
    #
    # @return [Numeric]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s2 = Polars::Series.new("b", [4.0, 5.0, 6.0])
    #   s.dot(s2)
    #   # => 32.0
    def dot(other)
      if !other.is_a?(Series)
        other = Series.new(other)
      end
      if len != other.len
        n, m = len, other.len
        raise ArgumentError, "Series length mismatch: expected #{n}, found #{m}"
      end
      _s.dot(other._s)
    end

    # Compute the most occurring value(s).
    #
    # Can return multiple Values.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.mode
    #   # =>
    #   # shape: (1,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   # ]
    def mode
      super
    end

    # Compute the element-wise indication of the sign.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [-9.0, -0.0, 0.0, 4.0, nil])
    #   s.sign
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         -1
    #   #         0
    #   #         0
    #   #         1
    #   #         null
    #   # ]
    def sign
      super
    end

    # Compute the element-wise value for the sine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [0.0, Math::PI / 2.0, Math::PI])
    #   s.sin
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.0
    #   #         1.0
    #   #         1.2246e-16
    #   # ]
    def sin
      super
    end

    # Compute the element-wise value for the cosine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [0.0, Math::PI / 2.0, Math::PI])
    #   s.cos
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.0
    #   #         6.1232e-17
    #   #         -1.0
    #   # ]
    def cos
      super
    end

    # Compute the element-wise value for the tangent.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [0.0, Math::PI / 2.0, Math::PI])
    #   s.tan
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.0
    #   #         1.6331e16
    #   #         -1.2246e-16
    #   # ]
    def tan
      super
    end

    # Compute the element-wise value for the inverse sine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.arcsin
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.570796
    #   #         0.0
    #   #         -1.570796
    #   # ]
    def arcsin
      super
    end

    # Compute the element-wise value for the inverse cosine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.arccos
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.0
    #   #         1.570796
    #   #         3.141593
    #   # ]
    def arccos
      super
    end

    # Compute the element-wise value for the inverse tangent.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.arctan
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.785398
    #   #         0.0
    #   #         -0.785398
    #   # ]
    def arctan
      super
    end

    # Compute the element-wise value for the inverse hyperbolic sine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.arcsinh
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.881374
    #   #         0.0
    #   #         -0.881374
    #   # ]
    def arcsinh
      super
    end

    # Compute the element-wise value for the inverse hyperbolic cosine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [5.0, 1.0, 0.0, -1.0])
    #   s.arccosh
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         2.292432
    #   #         0.0
    #   #         NaN
    #   #         NaN
    #   # ]
    def arccosh
      super
    end

    # Compute the element-wise value for the inverse hyperbolic tangent.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [2.0, 1.0, 0.5, 0.0, -0.5, -1.0, -1.1])
    #   s.arctanh
    #   # =>
    #   # shape: (7,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         NaN
    #   #         inf
    #   #         0.549306
    #   #         0.0
    #   #         -0.549306
    #   #         -inf
    #   #         NaN
    #   # ]
    def arctanh
      super
    end

    # Compute the element-wise value for the hyperbolic sine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.sinh
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.175201
    #   #         0.0
    #   #         -1.175201
    #   # ]
    def sinh
      super
    end

    # Compute the element-wise value for the hyperbolic cosine.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.cosh
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.543081
    #   #         1.0
    #   #         1.543081
    #   # ]
    def cosh
      super
    end

    # Compute the element-wise value for the hyperbolic tangent.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 0.0, -1.0])
    #   s.tanh
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         0.761594
    #   #         0.0
    #   #         -0.761594
    #   # ]
    def tanh
      super
    end

    # Apply a custom/user-defined function (UDF) over elements in this Series and
    # return a new Series.
    #
    # If the function returns another datatype, the return_dtype arg should be set,
    # otherwise the method will fail.
    #
    # @param return_dtype [Symbol]
    #   Output datatype. If none is given, the same datatype as this Series will be
    #   used.
    # @param skip_nulls [Boolean]
    #   Nulls will be skipped and not passed to the python function.
    #   This is faster because python can be skipped and because we call
    #   more specialized functions.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.apply { |x| x + 10 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         11
    #   #         12
    #   #         13
    #   # ]
    def apply(return_dtype: nil, skip_nulls: true, &func)
      if return_dtype.nil?
        pl_return_dtype = nil
      else
        pl_return_dtype = Utils.rb_type_to_dtype(return_dtype)
      end
      Utils.wrap_s(_s.apply_lambda(func, pl_return_dtype, skip_nulls))
    end

    # Shift the values by a given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.shift(1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         null
    #   #         1
    #   #         2
    #   # ]
    #
    # @example
    #   s.shift(-1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   #         3
    #   #         null
    #   # ]
    def shift(periods = 1)
      super
    end

    # Shift the values by a given period and fill the resulting null values.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   Fill None values with the result of this expression.
    #
    # @return [Series]
    def shift_and_fill(periods, fill_value)
      super
    end

    # Take values from self or other based on the given mask.
    #
    # Where mask evaluates true, take values from self. Where mask evaluates false,
    # take values from other.
    #
    # @param mask [Series]
    #   Boolean Series.
    # @param other [Series]
    #   Series of same type.
    #
    # @return [Series]
    #
    # @example
    #   s1 = Polars::Series.new([1, 2, 3, 4, 5])
    #   s2 = Polars::Series.new([5, 4, 3, 2, 1])
    #   s1.zip_with(s1 < s2, s2)
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         2
    #   #         1
    #   # ]
    #
    # @example
    #   mask = Polars::Series.new([true, false, true, false, true])
    #   s1.zip_with(mask, s2)
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [i64]
    #   # [
    #   #         1
    #   #         4
    #   #         3
    #   #         2
    #   #         5
    #   # ]
    def zip_with(mask, other)
      Utils.wrap_s(_s.zip_with(mask._s, other._s))
    end

    # Apply a rolling min (moving min) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [100, 200, 300, 400, 500])
    #   s.rolling_min(3)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         null
    #   #         null
    #   #         100
    #   #         200
    #   #         300
    #   # ]
    def rolling_min(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      to_frame
        .select(
          Polars.col(name).rolling_min(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Apply a rolling max (moving max) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [100, 200, 300, 400, 500])
    #   s.rolling_max(2)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         null
    #   #         200
    #   #         300
    #   #         400
    #   #         500
    #   # ]
    def rolling_max(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      to_frame
        .select(
          Polars.col(name).rolling_max(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Apply a rolling mean (moving mean) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [100, 200, 300, 400, 500])
    #   s.rolling_mean(2)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         150.0
    #   #         250.0
    #   #         350.0
    #   #         450.0
    #   # ]
    def rolling_mean(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      to_frame
        .select(
          Polars.col(name).rolling_mean(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Apply a rolling sum (moving sum) over the values in this array.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, 4, 5])
    #   s.rolling_sum(2)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         null
    #   #         3
    #   #         5
    #   #         7
    #   #         9
    #   # ]
    def rolling_sum(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      to_frame
        .select(
          Polars.col(name).rolling_sum(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Compute a rolling std dev.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, 4.0, 6.0, 8.0])
    #   s.rolling_std(3)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         1.0
    #   #         1.0
    #   #         1.527525
    #   #         2.0
    #   # ]
    def rolling_std(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      to_frame
        .select(
          Polars.col(name).rolling_std(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Compute a rolling variance.
    #
    # A window of length `window_size` will traverse the array. The values that fill
    # this window will (optionally) be multiplied with the weights given by the
    # `weight` vector. The resulting values will be aggregated to their sum.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, 4.0, 6.0, 8.0])
    #   s.rolling_var(3)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         1.0
    #   #         1.0
    #   #         2.333333
    #   #         4.0
    #   # ]
    def rolling_var(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      to_frame
        .select(
          Polars.col(name).rolling_var(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # def rolling_apply
    # end

    # Compute a rolling median.
    #
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, 4.0, 6.0, 8.0])
    #   s.rolling_median(3)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         2.0
    #   #         3.0
    #   #         4.0
    #   #         6.0
    #   # ]
    def rolling_median(
      window_size,
      weights: nil,
      min_periods: nil,
      center: false
    )
      if min_periods.nil?
        min_periods = window_size
      end

      to_frame
        .select(
          Polars.col(name).rolling_median(
            window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Compute a rolling quantile.
    #
    # @param quantile [Float]
    #   Quantile between 0.0 and 1.0.
    # @param interpolation ["nearest", "higher", "lower", "midpoint", "linear"]
    #   Interpolation method.
    # @param window_size [Integer]
    #   The length of the window.
    # @param weights [Array]
    #   An optional slice with the same length as the window that will be multiplied
    #   elementwise with the values in the window.
    # @param min_periods [Integer]
    #   The number of values in the window that should be non-null before computing
    #   a result. If None, it will be set equal to window size.
    # @param center [Boolean]
    #   Set the labels at the center of the window
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, 4.0, 6.0, 8.0])
    #   s.rolling_quantile(0.33, window_size: 3)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         1.0
    #   #         2.0
    #   #         3.0
    #   #         4.0
    #   # ]
    #
    # @example
    #   s.rolling_quantile(0.33, interpolation: "linear", window_size: 3)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         1.66
    #   #         2.66
    #   #         3.66
    #   #         5.32
    #   # ]
    def rolling_quantile(
      quantile,
      interpolation: "nearest",
      window_size: 2,
      weights: nil,
      min_periods: nil,
      center: false
    )
      if min_periods.nil?
        min_periods = window_size
      end

      to_frame
        .select(
          Polars.col(name).rolling_quantile(
            quantile,
            interpolation: interpolation,
            window_size: window_size,
            weights: weights,
            min_periods: min_periods,
            center: center
          )
        )
        .to_series
    end

    # Compute a rolling skew.
    #
    # @param window_size [Integer]
    #   Integer size of the rolling window.
    # @param bias [Boolean]
    #   If false, the calculations are corrected for statistical bias.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1.0, 2.0, 3.0, 4.0, 6.0, 8.0])
    #   s.rolling_skew(3)
    #   # =>
    #   # shape: (6,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         0.0
    #   #         0.0
    #   #         0.381802
    #   #         0.0
    #   # ]
    def rolling_skew(window_size, bias: true)
      super
    end

    # Sample from this Series.
    #
    # @param n [Integer]
    #   Number of items to return. Cannot be used with `frac`. Defaults to 1 if
    #   `frac` is None.
    # @param frac [Float]
    #   Fraction of items to return. Cannot be used with `n`.
    # @param with_replacement [Boolean]
    #   Allow values to be sampled more than once.
    # @param shuffle [Boolean]
    #   Shuffle the order of sampled data points.
    # @param seed [Integer]
    #   Seed for the random number generator. If set to None (default), a random
    #   seed is used.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, 4, 5])
    #   s.sample(n: 2, seed: 0)
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #     1
    #   #     5
    #   # ]
    def sample(
      n: nil,
      frac: nil,
      with_replacement: false,
      shuffle: false,
      seed: nil
    )
      if !n.nil? && !frac.nil?
        raise ArgumentError, "cannot specify both `n` and `frac`"
      end

      if n.nil? && !frac.nil?
        return Utils.wrap_s(_s.sample_frac(frac, with_replacement, shuffle, seed))
      end

      if n.nil?
        n = 1
      end
      Utils.wrap_s(_s.sample_n(n, with_replacement, shuffle, seed))
    end

    # Get a boolean mask of the local maximum peaks.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3, 4, 5])
    #   s.peak_max
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [bool]
    #   # [
    #   #         false
    #   #         false
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def peak_max
      Utils.wrap_s(_s.peak_max)
    end

    # Get a boolean mask of the local minimum peaks.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [4, 1, 3, 2, 5])
    #   s.peak_min
    #   # =>
    #   # shape: (5,)
    #   # Series: '' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         false
    #   #         true
    #   #         false
    #   # ]
    def peak_min
      Utils.wrap_s(_s.peak_min)
    end

    # Count the number of unique values in this Series.
    #
    # @return [Integer]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 2, 3])
    #   s.n_unique
    #   # => 3
    def n_unique
      _s.n_unique
    end

    # Shrink Series memory usage.
    #
    # Shrinks the underlying array capacity to exactly fit the actual data.
    # (Note that this function does not change the Series data type).
    #
    # @return [Series]
    def shrink_to_fit(in_place: false)
      if in_place
        _s.shrink_to_fit
        self
      else
        series = clone
        series._s.shrink_to_fit
        series
      end
    end

    # def _hash
    # end

    # Reinterpret the underlying bits as a signed/unsigned integer.
    #
    # This operation is only allowed for 64bit integers. For lower bits integers,
    # you can safely use that cast operation.
    #
    # @param signed [Boolean]
    #   If true, reinterpret as `:i64`. Otherwise, reinterpret as `:u64`.
    #
    # @return [Series]
    def reinterpret(signed: true)
      super
    end

    # Interpolate intermediate values. The interpolation method is linear.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, nil, nil, 5])
    #   s.interpolate
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   #         5
    #   # ]
    def interpolate
      super
    end

    # Compute absolute values.
    #
    # @return [Series]
    def abs
      super
    end

    # Assign ranks to data, dealing with ties appropriately.
    #
    # @param method ["average", "min", "max", "dense", "ordinal", "random"]
    #   The method used to assign ranks to tied elements.
    #   The following methods are available (default is 'average'):
    #
    #   - 'average' : The average of the ranks that would have been assigned to
    #     all the tied values is assigned to each value.
    #   - 'min' : The minimum of the ranks that would have been assigned to all
    #     the tied values is assigned to each value. (This is also referred to
    #     as "competition" ranking.)
    #   - 'max' : The maximum of the ranks that would have been assigned to all
    #     the tied values is assigned to each value.
    #   - 'dense' : Like 'min', but the rank of the next highest element is
    #     assigned the rank immediately after those assigned to the tied
    #     elements.
    #   - 'ordinal' : All values are given a distinct rank, corresponding to
    #     the order that the values occur in the Series.
    #   - 'random' : Like 'ordinal', but the rank for ties is not dependent
    #     on the order that the values occur in the Series.
    # @param reverse [Boolean]
    #   Reverse the operation.
    #
    # @return [Series]
    #
    # @example The 'average' method:
    #   s = Polars::Series.new("a", [3, 6, 1, 1, 6])
    #   s.rank
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [f32]
    #   # [
    #   #         3.0
    #   #         4.5
    #   #         1.5
    #   #         1.5
    #   #         4.5
    #   # ]
    #
    # @example The 'ordinal' method:
    #   s = Polars::Series.new("a", [3, 6, 1, 1, 6])
    #   s.rank(method: "ordinal")
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [u32]
    #   # [
    #   #         3
    #   #         4
    #   #         1
    #   #         2
    #   #         5
    #   # ]
    def rank(method: "average", reverse: false)
      super
    end

    # Calculate the n-th discrete difference.
    #
    # @param n [Integer]
    #   Number of slots to shift.
    # @param null_behavior ["ignore", "drop"]
    #   How to handle null values.
    #
    # @return [Series]
    def diff(n: 1, null_behavior: "ignore")
      super
    end

    # Computes percentage change between values.
    #
    # Percentage change (as fraction) between current element and most-recent
    # non-null element at least `n` period(s) before the current element.
    #
    # Computes the change from the previous row by default.
    #
    # @param n [Integer]
    #   periods to shift for forming percent change.
    #
    # @return [Series]
    #
    # @example
    #   Polars::Series.new(0..9).pct_change
    #   # =>
    #   # shape: (10,)
    #   # Series: '' [f64]
    #   # [
    #   #         null
    #   #         inf
    #   #         1.0
    #   #         0.5
    #   #         0.333333
    #   #         0.25
    #   #         0.2
    #   #         0.166667
    #   #         0.142857
    #   #         0.125
    #   # ]
    #
    # @example
    #   Polars::Series.new([1, 2, 4, 8, 16, 32, 64, 128, 256, 512]).pct_change(n: 2)
    #   # =>
    #   # shape: (10,)
    #   # Series: '' [f64]
    #   # [
    #   #         null
    #   #         null
    #   #         3.0
    #   #         3.0
    #   #         3.0
    #   #         3.0
    #   #         3.0
    #   #         3.0
    #   #         3.0
    #   #         3.0
    #   # ]
    def pct_change(n: 1)
      super
    end

    # Compute the sample skewness of a data set.
    #
    # For normally distributed data, the skewness should be about zero. For
    # unimodal continuous distributions, a skewness value greater than zero means
    # that there is more weight in the right tail of the distribution. The
    # function `skewtest` can be used to determine if the skewness value
    # is close enough to zero, statistically speaking.
    #
    # @param bias [Boolean]
    #   If `false`, the calculations are corrected for statistical bias.
    #
    # @return [Float, nil]
    def skew(bias: true)
      _s.skew(bias)
    end

    # Compute the kurtosis (Fisher or Pearson) of a dataset.
    #
    # Kurtosis is the fourth central moment divided by the square of the
    # variance. If Fisher's definition is used, then 3.0 is subtracted from
    # the result to give 0.0 for a normal distribution.
    # If bias is false, then the kurtosis is calculated using k statistics to
    # eliminate bias coming from biased moment estimators
    #
    # @param fisher [Boolean]
    #   If `true`, Fisher's definition is used (normal ==> 0.0). If `false`,
    #   Pearson's definition is used (normal ==> 3.0).
    # @param bias [Boolean]
    #   If `false`, the calculations are corrected for statistical bias.
    #
    # @return [Float, nil]
    def kurtosis(fisher: true, bias: true)
      _s.kurtosis(fisher, bias)
    end

    # Clip (limit) the values in an array to a `min` and `max` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See {#when} for more information.
    #
    # @param min_val [Numeric]
    #   Minimum value.
    # @param max_val [Numeric]
    #   Maximum value.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("foo", [-50, 5, nil, 50])
    #   s.clip(1, 10)
    #   # =>
    #   # shape: (4,)
    #   # Series: 'foo' [i64]
    #   # [
    #   #         1
    #   #         5
    #   #         null
    #   #         10
    #   # ]
    def clip(min_val, max_val)
      super
    end

    # Clip (limit) the values in an array to a `min` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See {#when} for more information.
    #
    # @param min_val [Numeric]
    #   Minimum value.
    #
    # @return [Series]
    def clip_min(min_val)
      super
    end

    # Clip (limit) the values in an array to a `max` boundary.
    #
    # Only works for numerical types.
    #
    # If you want to clip other dtypes, consider writing a "when, then, otherwise"
    # expression. See {#when} for more information.
    #
    # @param max_val [Numeric]
    #   Maximum value.
    #
    # @return [Series]
    def clip_max(max_val)
      super
    end

    # Reshape this Series to a flat Series or a Series of Lists.
    #
    # @param dims [Array]
    #   Tuple of the dimension sizes. If a -1 is used in any of the dimensions, that
    #   dimension is inferred.
    #
    # @return [Series]
    def reshape(dims)
      super
    end

    # Shuffle the contents of this Series.
    #
    # @param seed [Integer, nil]
    #   Seed for the random number generator.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.shuffle(seed: 1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         2
    #   #         1
    #   #         3
    #   # ]
    def shuffle(seed: nil)
      super
    end

    # Exponentially-weighted moving average.
    #
    # @return [Series]
    def ewm_mean(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      min_periods: 1
    )
      super
    end

    # Exponentially-weighted moving standard deviation.
    #
    # @return [Series]
    def ewm_std(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      bias: false,
      min_periods: 1
    )
      super
    end

    # Exponentially-weighted moving variance.
    #
    # @return [Series]
    def ewm_var(
      com: nil,
      span: nil,
      half_life: nil,
      alpha: nil,
      adjust: true,
      bias: false,
      min_periods: 1
    )
      super
    end

    # Extend the Series with given number of values.
    #
    # @param value [Object]
    #   The value to extend the Series with. This value may be `nil` to fill with
    #   nulls.
    # @param n [Integer]
    #   The number of values to extend.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.extend_constant(99, 2)
    #   # =>
    #   # shape: (5,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         99
    #   #         99
    #   # ]
    def extend_constant(value, n)
      super
    end

    # Flags the Series as sorted.
    #
    # Enables downstream code to user fast paths for sorted arrays.
    #
    # @param reverse [Boolean]
    #   If the Series order is reversed, e.g. descending.
    #
    # @return [Series]
    #
    # @note
    #   This can lead to incorrect results if this Series is not sorted!!
    #   Use with care!
    #
    # @example
    #   s = Polars::Series.new("a", [1, 2, 3])
    #   s.set_sorted.max
    #   # => 3
    def set_sorted(reverse: false)
      Utils.wrap_s(_s.set_sorted(reverse))
    end

    # Create a new Series filled with values from the given index.
    #
    # @return [Series]
    def new_from_index(index, length)
      Utils.wrap_s(_s.new_from_index(index, length))
    end

    # Shrink numeric columns to the minimal required datatype.
    #
    # Shrink to the dtype needed to fit the extrema of this Series.
    # This can be used to reduce memory pressure.
    #
    # @return [Series]
    def shrink_dtype
      super
    end

    # Create an object namespace of all list related methods.
    #
    # @return [ListNameSpace]
    def arr
      ListNameSpace.new(self)
    end

    # Create an object namespace of all categorical related methods.
    #
    # @return [CatNameSpace]
    def cat
      CatNameSpace.new(self)
    end

    # Create an object namespace of all datetime related methods.
    #
    # @return [DateTimeNameSpace]
    def dt
      DateTimeNameSpace.new(self)
    end

    # Create an object namespace of all string related methods.
    #
    # @return [StringNameSpace]
    def str
      StringNameSpace.new(self)
    end

    # Create an object namespace of all struct related methods.
    #
    # @return [StructNameSpace]
    def struct
      StructNameSpace.new(self)
    end

    private

    def initialize_copy(other)
      super
      self._s = _s._clone
    end

    def coerce(other)
      if other.is_a?(Numeric)
        # TODO improve
        series = to_frame.select(Polars.lit(other)).to_series
        [series, self]
      else
        raise TypeError, "#{self.class} can't be coerced into #{other.class}"
      end
    end

    def _comp(other, op)
      if other.is_a?(Series)
        return Utils.wrap_s(_s.send(op, other._s))
      end

      if dtype == :str
        raise Todo
      end
      Utils.wrap_s(_s.send("#{op}_#{dtype}", other))
    end

    def _arithmetic(other, op)
      if other.is_a?(Expr)
        other = to_frame.select(other).to_series
      end
      if other.is_a?(Series)
        return Utils.wrap_s(_s.send(op, other._s))
      end

      raise Todo
    end

    def series_to_rbseries(name, values)
      # should not be in-place?
      values.rename(name, in_place: true)
      values._s
    end

    def sequence_to_rbseries(name, values, dtype: nil, strict: true, dtype_if_empty: nil)
      ruby_dtype = nil
      nested_dtype = nil

      if (values.nil? || values.empty?) && dtype.nil?
        if dtype_if_empty
          # if dtype for empty sequence could be guessed
          # (e.g comparisons between self and other)
          dtype = dtype_if_empty
        else
          # default to Float32 type
          dtype = :f32
        end
      end

      rb_temporal_types = []
      rb_temporal_types << Date if defined?(Date)
      rb_temporal_types << DateTime if defined?(DateTime)
      rb_temporal_types << Time if defined?(Time)

      value = _get_first_non_none(values)

      if !dtype.nil? && Utils.is_polars_dtype(dtype) && ruby_dtype.nil?
        constructor = polars_type_to_constructor(dtype)
        rbseries = constructor.call(name, values, strict)
        return rbseries
      else
        if ruby_dtype.nil?
          if value.nil?
            # generic default dtype
            ruby_dtype = Float
          else
            ruby_dtype = value.class
          end
        end

        # temporal branch
        if rb_temporal_types.include?(ruby_dtype)
          # if dtype.nil?
          #   dtype = rb_type_to_dtype(ruby_dtype)
          # elsif rb_temporal_types.include?(dtype)
          #   dtype = rb_type_to_dtype(dtype)
          # end

          if ruby_dtype == Date
            RbSeries.new_opt_date(name, values, strict)
          elsif ruby_dtype == Time
            RbSeries.new_opt_datetime(name, values, strict)
          elsif ruby_dtype == DateTime
            RbSeries.new_opt_datetime(name, values.map(&:to_time), strict)
          else
            raise Todo
          end
        elsif ruby_dtype == Array
          if nested_dtype.nil?
            nested_value = _get_first_non_none(value)
            nested_dtype = nested_value.nil? ? Float : nested_value.class
          end

          if nested_dtype == Array
            raise Todo
          end

          if value.is_a?(Array)
            count = 0
            equal_to_inner = true
            values.each do |lst|
              lst.each do |vl|
                equal_to_inner = vl.class == nested_dtype
                if !equal_to_inner || count > 50
                  break
                end
                count += 1
              end
            end
            if equal_to_inner
              dtype = Utils.rb_type_to_dtype(nested_dtype)
              # TODO rescue and fallback to new_object
              return RbSeries.new_list(name, values, dtype)
            end
          end

          RbSeries.new_object(name, values, strict)
        else
          constructor = rb_type_to_constructor(value.class)
          constructor.call(name, values, strict)
        end
      end
    end

    POLARS_TYPE_TO_CONSTRUCTOR = {
      f32: RbSeries.method(:new_opt_f32),
      f64: RbSeries.method(:new_opt_f64),
      i8: RbSeries.method(:new_opt_i8),
      i16: RbSeries.method(:new_opt_i16),
      i32: RbSeries.method(:new_opt_i32),
      i64: RbSeries.method(:new_opt_i64),
      u8: RbSeries.method(:new_opt_u8),
      u16: RbSeries.method(:new_opt_u16),
      u32: RbSeries.method(:new_opt_u32),
      u64: RbSeries.method(:new_opt_u64),
      bool: RbSeries.method(:new_opt_bool),
      str: RbSeries.method(:new_str)
    }

    def polars_type_to_constructor(dtype)
      POLARS_TYPE_TO_CONSTRUCTOR.fetch(dtype.to_sym)
    rescue KeyError
      raise ArgumentError, "Cannot construct RbSeries for type #{dtype}."
    end

    RB_TYPE_TO_CONSTRUCTOR = {
      Float => RbSeries.method(:new_opt_f64),
      Integer => RbSeries.method(:new_opt_i64),
      String => RbSeries.method(:new_str),
      TrueClass => RbSeries.method(:new_opt_bool),
      FalseClass => RbSeries.method(:new_opt_bool)
    }

    def rb_type_to_constructor(dtype)
      RB_TYPE_TO_CONSTRUCTOR.fetch(dtype)
    rescue KeyError
      RbSeries.method(:new_object)
    end

    def _get_first_non_none(values)
      values.find { |v| !v.nil? }
    end
  end
end

module Polars
  # A Series represents a single column in a polars DataFrame.
  class Series
    # @private
    attr_accessor :_s

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
      Utils.wrap_s(_s.bitand(other._s))
    end

    # Bitwise OR.
    #
    # @return [Series]
    def |(other)
      Utils.wrap_s(_s.bitor(other._s))
    end

    # Bitwise XOR.
    #
    # @return [Series]
    def ^(other)
      Utils.wrap_s(_s.bitxor(other._s))
    end

    # def ==(other)
    # end

    # def !=(other)
    # end

    # def >(other)
    # end

    # def <(other)
    # end

    # def >=(other)
    # end

    # def <=(other)
    # end

    # Performs addition.
    #
    # @return [Series]
    def +(other)
     Utils. wrap_s(_s.add(other._s))
    end

    # Performs subtraction.
    #
    # @return [Series]
    def -(other)
      Utils.wrap_s(_s.sub(other._s))
    end

    # Performs multiplication.
    #
    # @return [Series]
    def *(other)
      Utils.wrap_s(_s.mul(other._s))
    end

    # Performs division.
    #
    # @return [Series]
    def /(other)
      Utils.wrap_s(_s.div(other._s))
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

    # def -@(other)
    # end

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

    # def log
    # end

    # def log10
    # end

    # def exp
    # end

    # def drop_nulls
    # end

    # def drop_nans
    # end

    # Cast this Series to a DataFrame.
    #
    # @return [DataFrame]
    def to_frame
      Utils.wrap_df(RbDataFrame.new([_s]))
    end

    # def describe
    # end

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

    # def nan_max
    # end

    # def nan_min
    # end

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

    # def unique_counts
    # end

    # def entropy
    # end

    # def cumulative_eval
    # end

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
      Utils.wrap_s(_s.cumsum(reverse))
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
      Utils.wrap_s(_s.cummin(reverse))
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
      Utils.wrap_s(_s.cummax(reverse))
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
      Utils.wrap_s(_s.cumprod(reverse))
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
      length = len if length.nil?
      Utils.wrap_s(_s.slice(offset, length))
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

    # def take_every
    # end

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

    # def top_k
    # end

    # def arg_sort
    # end

    # def argsort
    # end

    # def arg_unique
    # end

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

    # def search_sorted
    # end

    # def unique
    # end

    # def take
    # end

    # Count the null values in this Series.
    #
    # @return [Integer]
    def null_count
      _s.null_count
    end

    # Return True if the Series has a validity bitmask.
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

    # def is_null
    # end

    # def is_not_null
    # end

    # def is_finite
    # end

    # def is_infinite
    # end

    # def is_nan
    # end

    # def is_not_nan
    # end

    # def is_in
    # end

    # def arg_true
    # end

    # def is_unique
    # end

    # def is_first
    # end

    # def is_duplicated
    # end

    # def explode
    # end

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
      Utils.wrap_s(_s.cast(dtype.to_s, strict))
    end

    # def to_physical
    # end

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

    # def reverse
    # end

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

    # def fill_nan
    # end

    # def fill_null
    # end

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
      Utils.wrap_s(_s.ceil)
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
      Utils.wrap_s(_s.round(decimals))
    end

    # def dot
    # end

    # def mode
    # end

    # def sign
    # end

    # def sin
    # end

    # def cos
    # end

    # def tan
    # end

    # def arcsin
    # end

    # def arccos
    # end

    # def arctan
    # end

    # def arcsinh
    # end

    # def arccosh
    # end

    # def arctanh
    # end

    # def sinh
    # end

    # def cosh
    # end

    # def tanh
    # end

    # def apply
    # end

    # def shift
    # end

    # def shift_and_fill
    # end

    # def zip_with
    # end

    # def rolling_min
    # end

    # def rolling_max
    # end

    # def rolling_mean
    # end

    # def rolling_sum
    # end

    # def rolling_std
    # end

    # def rolling_var
    # end

    # def rolling_apply
    # end

    # def rolling_median
    # end

    # def rolling_quantile
    # end

    # def rolling_skew
    # end

    # def sample
    # end

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

    # def shrink_to_fit
    # end

    # def _hash
    # end

    # def reinterpret
    # end

    # def interpolate
    # end

    # def abs
    # end

    # def rank
    # end

    # def diff
    # end

    # def pct_change
    # end

    # def skew
    # end

    # def kurtosis
    # end

    # def clip
    # end

    # def clip_min
    # end

    # def clip_max
    # end

    # def reshape
    # end

    # def shuffle
    # end

    # def ewm_mean
    # end

    # def ewm_std
    # end

    # def ewm_var
    # end

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
      Utils.wrap_s(_s.extend_constant(value, n))
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

    # def new_from_index
    # end

    # def shrink_dtype
    # end

    # def arr
    # end

    # def cat
    # end

    # def dt
    # end

    # def str
    # end

    # def struct
    # end

    private

    def initialize_copy(other)
      super
      self._s = _s._clone
    end

    def series_to_rbseries(name, values)
      # should not be in-place?
      values.rename(name, in_place: true)
      values._s
    end

    def sequence_to_rbseries(name, values, dtype: nil, strict: true, dtype_if_empty: nil)
      ruby_dtype = nil

      if (values.nil? || values.empty?) && dtype.nil?
        if dtype_if_empty
          # if dtype for empty sequence could be guessed
          # (e.g comparisons between self and other)
          dtype = dtype_if_empty
        else
          # default to Float32 type
          dtype = "f32"
        end
      end

      rb_temporal_types = []
      rb_temporal_types << Date if defined?(Date)
      rb_temporal_types << DateTime if defined?(DateTime)
      rb_temporal_types << Time if defined?(Time)

      # _get_first_non_none
      value = values.find { |v| !v.nil? }

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

          raise Todo
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
      # RbSeries.method(:new_object)
      raise ArgumentError, "Cannot determine type"
    end
  end
end

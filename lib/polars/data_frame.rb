module Polars
  # Two-dimensional data structure representing data as a table with rows and columns.
  class DataFrame
    include Plot

    # @private
    attr_accessor :_df

    # Create a new DataFrame.
    #
    # @param data [Object]
    #   Two-dimensional data in various forms; hash input must contain arrays
    #   or a range. Arrays may contain Series or other arrays.
    # @param schema [Object]
    #   The schema of the resulting DataFrame. The schema may be declared in several
    #   ways:
    #
    #   * As a hash of \\\\{name:type} pairs; if type is nil, it will be auto-inferred.
    #   * As an array of column names; in this case types are automatically inferred.
    #   * As an array of (name,type) pairs; this is equivalent to the hash form.
    #
    #   If you supply an array of column names that does not match the names in the
    #   underlying data, the names given here will overwrite them. The number
    #   of names given in the schema should match the underlying data dimensions.
    #
    #   If set to `nil` (default), the schema is inferred from the data.
    # @param schema_overrides [Hash]
    #   Support type specification or override of one or more columns; note that
    #   any dtypes inferred from the schema param will be overridden.
    #
    #   The number of entries in the schema should match the underlying data
    #   dimensions, unless an array of hashes is being passed, in which case
    #   a *partial* schema can be declared to prevent specific fields from being loaded.
    # @param strict [Boolean]
    #   Throw an error if any `data` value does not exactly match the given or inferred
    #   data type for that column. If set to `false`, values that do not match the data
    #   type are cast to that data type or, if casting is not possible, set to null
    #   instead.
    # @param orient ["col", "row"]
    #   Whether to interpret two-dimensional data as columns or as rows. If nil,
    #   the orientation is inferred by matching the columns and data dimensions. If
    #   this does not yield conclusive results, column orientation is used.
    # @param infer_schema_length [Integer]
    #   The maximum number of rows to scan for schema inference. If set to `nil`, the
    #   full data may be scanned *(this can be slow)*. This parameter only applies if
    #   the input data is a sequence or generator of rows; other input is read as-is.
    # @param nan_to_null [Boolean]
    #   If the data comes from one or more Numo arrays, can optionally convert input
    #   data NaN values to null instead. This is a no-op for all other input data.
    def initialize(data = nil, schema: nil, schema_overrides: nil, strict: true, orient: nil, infer_schema_length: 100, nan_to_null: false)
      if defined?(ActiveRecord) && (data.is_a?(ActiveRecord::Relation) || data.is_a?(ActiveRecord::Result))
        raise ArgumentError, "Use read_database instead"
      end

      if data.nil?
        self._df = self.class.hash_to_rbdf({}, schema: schema, schema_overrides: schema_overrides)
      elsif data.is_a?(Hash)
        data = data.transform_keys { |v| v.is_a?(Symbol) ? v.to_s : v }
        self._df = self.class.hash_to_rbdf(data, schema: schema, schema_overrides: schema_overrides, strict: strict, nan_to_null: nan_to_null)
      elsif data.is_a?(::Array)
        self._df = self.class.sequence_to_rbdf(data, schema: schema, schema_overrides: schema_overrides, strict: strict, orient: orient, infer_schema_length: infer_schema_length)
      elsif data.is_a?(Series)
        self._df = self.class.series_to_rbdf(data, schema: schema, schema_overrides: schema_overrides, strict: strict)
      elsif data.respond_to?(:arrow_c_stream)
        # This uses the fact that RbSeries.from_arrow_c_stream will create a
        # struct-typed Series. Then we unpack that to a DataFrame.
        tmp_col_name = ""
        s = Utils.wrap_s(RbSeries.from_arrow_c_stream(data))
        self._df = s.to_frame(tmp_col_name).unnest(tmp_col_name)._df
      else
        raise ArgumentError, "DataFrame constructor called with unsupported type; got #{data.class.name}"
      end
    end

    # Read a serialized DataFrame from a file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object (by file-like object, we refer to
    #   objects that have a `read` method, such as a file handler or `StringIO`).
    #
    # @return [DataFrame]
    #
    # @note
    #   Serialization is not stable across Polars versions: a LazyFrame serialized
    #   in one Polars version may not be deserializable in another Polars version.
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4.0, 5.0, 6.0]})
    #   bytes = df.serialize
    #   Polars::DataFrame.deserialize(StringIO.new(bytes))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4.0 │
    #   # │ 2   ┆ 5.0 │
    #   # │ 3   ┆ 6.0 │
    #   # └─────┴─────┘
    def self.deserialize(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      deserializer = RbDataFrame.method(:deserialize_binary)

      _from_rbdf(deserializer.(source))
    end

    # @private
    def self._from_rbdf(rb_df)
      df = DataFrame.allocate
      df._df = rb_df
      df
    end

    # Get the shape of the DataFrame.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5]})
    #   df.shape
    #   # => [5, 1]
    def shape
      _df.shape
    end

    # Get the height of the DataFrame.
    #
    # @return [Integer]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5]})
    #   df.height
    #   # => 5
    def height
      _df.height
    end
    alias_method :count, :height
    alias_method :length, :height
    alias_method :size, :height

    # Get the width of the DataFrame.
    #
    # @return [Integer]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3, 4, 5]})
    #   df.width
    #   # => 1
    def width
      _df.width
    end

    # Get column names.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.columns
    #   # => ["foo", "bar", "ham"]
    def columns
      _df.columns
    end

    # Change the column names of the DataFrame.
    #
    # @param columns [Array]
    #   A list with new names for the DataFrame.
    #   The length of the list should be equal to the width of the DataFrame.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.columns = ["apple", "banana", "orange"]
    #   df
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬────────┬────────┐
    #   # │ apple ┆ banana ┆ orange │
    #   # │ ---   ┆ ---    ┆ ---    │
    #   # │ i64   ┆ i64    ┆ str    │
    #   # ╞═══════╪════════╪════════╡
    #   # │ 1     ┆ 6      ┆ a      │
    #   # │ 2     ┆ 7      ┆ b      │
    #   # │ 3     ┆ 8      ┆ c      │
    #   # └───────┴────────┴────────┘
    def columns=(columns)
      _df.set_column_names(columns)
    end

    # Get dtypes of columns in DataFrame. Dtypes can also be found in column headers when printing the DataFrame.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.dtypes
    #   # => [Polars::Int64, Polars::Float64, Polars::String]
    def dtypes
      _df.dtypes
    end

    # Get flags that are set on the columns of this DataFrame.
    #
    # @return [Hash]
    def flags
      columns.to_h { |name| [name, self[name].flags] }
    end

    # Get the schema.
    #
    # @return [Hash]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.schema
    #   # => {"foo"=>Polars::Int64, "bar"=>Polars::Float64, "ham"=>Polars::String}
    def schema
      columns.zip(dtypes).to_h
    end

    # Equal.
    #
    # @return [DataFrame]
    def ==(other)
      _comp(other, "eq")
    end

    # Not equal.
    #
    # @return [DataFrame]
    def !=(other)
      _comp(other, "neq")
    end

    # Greater than.
    #
    # @return [DataFrame]
    def >(other)
      _comp(other, "gt")
    end

    # Less than.
    #
    # @return [DataFrame]
    def <(other)
      _comp(other, "lt")
    end

    # Greater than or equal.
    #
    # @return [DataFrame]
    def >=(other)
      _comp(other, "gt_eq")
    end

    # Less than or equal.
    #
    # @return [DataFrame]
    def <=(other)
      _comp(other, "lt_eq")
    end

    # Performs multiplication.
    #
    # @return [DataFrame]
    def *(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.mul_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.mul(other._s))
    end

    # Performs division.
    #
    # @return [DataFrame]
    def /(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.div_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.div(other._s))
    end

    # Performs addition.
    #
    # @return [DataFrame]
    def +(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.add_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.add(other._s))
    end

    # Performs subtraction.
    #
    # @return [DataFrame]
    def -(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.sub_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.sub(other._s))
    end

    # Returns the modulo.
    #
    # @return [DataFrame]
    def %(other)
      if other.is_a?(DataFrame)
        return _from_rbdf(_df.rem_df(other._df))
      end

      other = _prepare_other_arg(other)
      _from_rbdf(_df.rem(other._s))
    end

    # Returns a string representing the DataFrame.
    #
    # @return [String]
    def to_s
      _df.to_s
    end
    alias_method :inspect, :to_s

    # Returns an array representing the DataFrame
    #
    # @return [Array]
    def to_a
      rows(named: true)
    end

    # Check if DataFrame includes column.
    #
    # @return [Boolean]
    def include?(name)
      columns.include?(name)
    end

    # Returns an enumerator.
    #
    # @return [Object]
    def each(&block)
      get_columns.each(&block)
    end

    # Returns subset of the DataFrame.
    #
    # @return [Object]
    def [](*args)
      if args.size == 2
        row_selection, col_selection = args

        # df[.., unknown]
        if row_selection.is_a?(Range)

          # multiple slices
          # df[.., ..]
          if col_selection.is_a?(Range)
            raise Todo
          end
        end

        # df[2, ..] (select row as df)
        if row_selection.is_a?(Integer)
          if col_selection.is_a?(::Array)
            df = self[0.., col_selection]
            return df.slice(row_selection, 1)
          end
          # df[2, "a"]
          if col_selection.is_a?(::String) || col_selection.is_a?(Symbol)
            return self[col_selection][row_selection]
          end
        end

        # column selection can be "a" and ["a", "b"]
        if col_selection.is_a?(::String) || col_selection.is_a?(Symbol)
          col_selection = [col_selection]
        end

        # df[.., 1]
        if col_selection.is_a?(Integer)
          series = to_series(col_selection)
          return series[row_selection]
        end

        if col_selection.is_a?(::Array)
          # df[.., [1, 2]]
          if Utils.is_int_sequence(col_selection)
            series_list = col_selection.map { |i| to_series(i) }
            df = self.class.new(series_list)
            return df[row_selection]
          end
        end

        df = self[col_selection]
        return df[row_selection]
      elsif args.size == 1
        item = args[0]

        # select single column
        # df["foo"]
        if item.is_a?(::String) || item.is_a?(Symbol)
          return Utils.wrap_s(_df.get_column(item.to_s))
        end

        # df[idx]
        if item.is_a?(Integer)
          return slice(_pos_idx(item, 0), 1)
        end

        # df[..]
        if item.is_a?(Range)
          return Slice.new(self).apply(item)
        end

        if item.is_a?(::Array) && item.all? { |v| Utils.strlike?(v) }
          # select multiple columns
          # df[["foo", "bar"]]
          return _from_rbdf(_df.select(item.map(&:to_s)))
        end

        if Utils.is_int_sequence(item)
          item = Series.new("", item)
        end

        if item.is_a?(Series)
          dtype = item.dtype
          if dtype == String
            return _from_rbdf(_df.select(item))
          elsif dtype == UInt32
            return _from_rbdf(_df.take_with_series(item._s))
          elsif [UInt8, UInt16, UInt64, Int8, Int16, Int32, Int64].include?(dtype)
            return _from_rbdf(
              _df.take_with_series(_pos_idxs(item, 0)._s)
            )
          end
        end
      end

      # Ruby-specific
      if item.is_a?(Expr) || item.is_a?(Series)
        return filter(item)
      end

      raise ArgumentError, "Cannot get item of type: #{item.class.name}"
    end

    # Set item.
    #
    # @return [Object]
    def []=(*key, value)
      if key.length == 1
        key = key.first
      elsif key.length != 2
        raise ArgumentError, "wrong number of arguments (given #{key.length + 1}, expected 2..3)"
      end

      if Utils.strlike?(key)
        if value.is_a?(::Array) || (defined?(Numo::NArray) && value.is_a?(Numo::NArray))
          value = Series.new(value)
        elsif !value.is_a?(Series)
          value = Polars.lit(value)
        end
        self._df = with_columns(value.alias(key.to_s))._df
      elsif key.is_a?(::Array)
        row_selection, col_selection = key

        if Utils.strlike?(col_selection)
          s = self[col_selection]
        elsif col_selection.is_a?(Integer)
          raise Todo
        else
          raise ArgumentError, "column selection not understood: #{col_selection}"
        end

        s[row_selection] = value

        if col_selection.is_a?(Integer)
          replace_column(col_selection, s)
        elsif Utils.strlike?(col_selection)
          _replace(col_selection.to_s, s)
        end
      else
        raise Todo
      end
    end

    # @private
    def arrow_c_stream
      _df.arrow_c_stream
    end

    # Get an ordered mapping of column names to their data type.
    #
    # @return [Schema]
    #
    # @note
    #   This method is included to facilitate writing code that is generic for both
    #   DataFrame and LazyFrame.
    #
    # @example Determine the schema.
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.collect_schema
    #   # => Polars::Schema({"foo"=>Polars::Int64, "bar"=>Polars::Float64, "ham"=>Polars::String})
    #
    # @example Access various properties of the schema using the `Schema` object.
    #   schema = df.collect_schema
    #   schema["bar"]
    #   # => Polars::Float64
    #
    # @example
    #   schema.names
    #   # => ["foo", "bar", "ham"]
    #
    # @example
    #   schema.dtypes
    #   # => [Polars::Int64, Polars::Float64, Polars::String]
    #
    # @example
    #   schema.length
    #   # => 3
    def collect_schema
      Schema.new(columns.zip(dtypes), check_dtypes: false)
    end

    # Return the dataframe as a scalar.
    #
    # Equivalent to `df[0,0]`, with a check that the shape is (1,1).
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [4, 5, 6]})
    #   result = df.select((Polars.col("a") * Polars.col("b")).sum)
    #   result.item
    #   # => 32
    def item
      if shape != [1, 1]
        raise ArgumentError, "Can only call .item if the dataframe is of shape (1,1), dataframe is of shape #{shape}"
      end
      self[0, 0]
    end

    # no to_arrow

    # Convert DataFrame to a hash mapping column name to values.
    #
    # @return [Hash]
    def to_h(as_series: true)
      if as_series
        get_columns.to_h { |s| [s.name, s] }
      else
        get_columns.to_h { |s| [s.name, s.to_a] }
      end
    end

    # Convert every row to a hash.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.to_hashes
    #   # =>
    #   # [{"foo"=>1, "bar"=>4}, {"foo"=>2, "bar"=>5}, {"foo"=>3, "bar"=>6}]
    def to_hashes
      rows(named: true)
    end

    # Convert DataFrame to a 2D Numo array.
    #
    # This operation clones data.
    #
    # @return [Numo::NArray]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => [1, 2, 3], "bar" => [6, 7, 8], "ham" => ["a", "b", "c"]}
    #   )
    #   df.to_numo.class
    #   # => Numo::RObject
    def to_numo
      out = _df.to_numo
      if out.nil?
        Numo::NArray.vstack(width.times.map { |i| to_series(i).to_numo }).transpose
      else
        out
      end
    end

    # no to_pandas

    # Select column as Series at index location.
    #
    # @param index [Integer]
    #   Location of selection.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.to_series(1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'bar' [i64]
    #   # [
    #   #         6
    #   #         7
    #   #         8
    #   # ]
    def to_series(index = 0)
      if index < 0
        index = columns.length + index
      end
      Utils.wrap_s(_df.select_at_idx(index))
    end

    # Serialize this DataFrame to a file or string.
    #
    # @param file [Object]
    #   File path or writable file-like object to which the result will be written.
    #   If set to `nil` (default), the output is returned as a string instead.
    #
    # @return [Object]
    #
    # @note
    #   Serialization is not stable across Polars versions: a LazyFrame serialized
    #   in one Polars version may not be deserializable in another Polars version.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8]
    #     }
    #   )
    #   bytes = df.serialize
    #   Polars::DataFrame.deserialize(StringIO.new(bytes))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6   │
    #   # │ 2   ┆ 7   │
    #   # │ 3   ┆ 8   │
    #   # └─────┴─────┘
    def serialize(file = nil)
      serializer = _df.method(:serialize_binary)

      Utils.serialize_polars_object(serializer, file)
    end

    # Serialize to JSON representation.
    #
    # @param file [String]
    #   File path to which the result should be written.
    #
    # @return [nil]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8]
    #     }
    #   )
    #   df.write_json
    #   # => "[{\"foo\":1,\"bar\":6},{\"foo\":2,\"bar\":7},{\"foo\":3,\"bar\":8}]"
    def write_json(file = nil)
      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end
      to_string_io = !file.nil? && file.is_a?(StringIO)
      if file.nil? || to_string_io
        buf = StringIO.new
        buf.set_encoding(Encoding::BINARY)
        _df.write_json(buf)
        json_bytes = buf.string

        json_str = json_bytes.force_encoding(Encoding::UTF_8)
        if to_string_io
          file.write(json_str)
        else
          return json_str
        end
      else
        _df.write_json(file)
      end
      nil
    end

    # Serialize to newline delimited JSON representation.
    #
    # @param file [String]
    #   File path to which the result should be written.
    #
    # @return [nil]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8]
    #     }
    #   )
    #   df.write_ndjson
    #   # => "{\"foo\":1,\"bar\":6}\n{\"foo\":2,\"bar\":7}\n{\"foo\":3,\"bar\":8}\n"
    def write_ndjson(file = nil)
      should_return_buffer = false
      target = nil
      if file.nil?
        target = StringIO.new
        target.set_encoding(Encoding::BINARY)
        should_return_buffer = true
      elsif Utils.pathlike?(file)
        target = Utils.normalize_filepath(file)
      else
        target = file
      end

      lazy.sink_ndjson(
        target
      )

      if should_return_buffer
        return target.string.force_encoding(Encoding::UTF_8)
      end

      nil
    end

    # Write to comma-separated values (CSV) file.
    #
    # @param file [String, nil]
    #   File path to which the result should be written. If set to `nil`
    #   (default), the output is returned as a string instead.
    # @param include_header [Boolean]
    #   Whether to include header in the CSV output.
    # @param separator [String]
    #   Separate CSV fields with this symbol.
    # @param quote_char [String]
    #   Byte to use as quoting character.
    # @param batch_size [Integer]
    #   Number of rows that will be processed per thread.
    # @param datetime_format [String, nil]
    #   A format string, with the specifiers defined by the
    #   [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   Rust crate. If no format specified, the default fractional-second
    #   precision is inferred from the maximum timeunit found in the frame's
    #   Datetime cols (if any).
    # @param date_format [String, nil]
    #   A format string, with the specifiers defined by the
    #   [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   Rust crate.
    # @param time_format [String, nil]
    #   A format string, with the specifiers defined by the
    #   [chrono](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   Rust crate.
    # @param float_precision [Integer, nil]
    #   Number of decimal places to write, applied to both `:f32` and
    #   `:f64` datatypes.
    # @param null_value [String, nil]
    #   A string representing null values (defaulting to the empty string).
    #
    # @return [String, nil]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.write_csv("file.csv")
    def write_csv(
      file = nil,
      include_bom: false,
      include_header: true,
      separator: ",",
      line_terminator: "\n",
      quote_char: '"',
      batch_size: 1024,
      datetime_format: nil,
      date_format: nil,
      time_format: nil,
      float_scientific: nil,
      float_precision: nil,
      decimal_comma: false,
      null_value: nil,
      quote_style: nil,
      storage_options: nil,
      credential_provider: "auto",
      retries: 2
    )
      Utils._check_arg_is_1byte("separator", separator, false)
      Utils._check_arg_is_1byte("quote_char", quote_char, true)
      if null_value == ""
        null_value = nil
      end

      if file.nil?
        buffer = StringIO.new
        buffer.set_encoding(Encoding::BINARY)
        lazy.sink_csv(
          buffer,
          include_bom: include_bom,
          include_header: include_header,
          separator: separator,
          line_terminator: line_terminator,
          quote_char: quote_char,
          batch_size: batch_size,
          datetime_format: datetime_format,
          date_format: date_format,
          time_format: time_format,
          float_scientific: float_scientific,
          float_precision: float_precision,
          decimal_comma: decimal_comma,
          null_value: null_value,
          quote_style: quote_style,
          storage_options: storage_options,
          credential_provider: credential_provider,
          retries: retries
        )
        return buffer.string.force_encoding(Encoding::UTF_8)
      end

      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end

      lazy.sink_csv(
        file,
        include_bom: include_bom,
        include_header: include_header,
        separator: separator,
        line_terminator: line_terminator,
        quote_char: quote_char,
        batch_size: batch_size,
        datetime_format: datetime_format,
        date_format: date_format,
        time_format: time_format,
        float_scientific: float_scientific,
        float_precision: float_precision,
        decimal_comma: decimal_comma,
        null_value: null_value,
        quote_style: quote_style,
        storage_options: storage_options,
        credential_provider: credential_provider,
        retries: retries
      )
      nil
    end

    # Write to comma-separated values (CSV) string.
    #
    # @return [String]
    def to_csv(**options)
      write_csv(**options)
    end

    # Write to Apache Avro file.
    #
    # @param file [String]
    #   File path to which the file should be written.
    # @param compression ["uncompressed", "snappy", "deflate"]
    #   Compression method. Defaults to "uncompressed".
    # @param name [String]
    #   Schema name. Defaults to empty string.
    #
    # @return [nil]
    def write_avro(file, compression = "uncompressed", name: "")
      if compression.nil?
        compression = "uncompressed"
      end
      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end
      if name.nil?
        name = ""
      end

      _df.write_avro(file, compression, name)
    end

    # Write to Arrow IPC binary stream or Feather file.
    #
    # @param file [String]
    #   File path to which the file should be written.
    # @param compression ["uncompressed", "lz4", "zstd"]
    #   Compression method. Defaults to "uncompressed".
    # @param compat_level [Object]
    #   Use a specific compatibility level
    #   when exporting Polars' internal data structures.
    # @param storage_options [Hash]
    #   Options that indicate how to connect to a cloud provider.
    #
    #   The cloud providers currently supported are AWS, GCP, and Azure.
    #   See supported keys here:
    #
    #   * [aws](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
    #   * [gcp](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)
    #   * [azure](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
    #   * Hugging Face (`hf://`): Accepts an API key under the `token` parameter: `{'token': '...'}`, or by setting the `HF_TOKEN` environment variable.
    #
    #   If `storage_options` is not provided, Polars will try to infer the
    #   information from environment variables.
    # @param credential_provider [Object]
    #   Provide a function that can be called to provide cloud storage
    #   credentials. The function is expected to return a dictionary of
    #   credential keys along with an optional credential expiry time.
    # @param retries [Integer]
    #   Number of retries if accessing a cloud instance fails.
    #
    # @return [nil]
    def write_ipc(
      file,
      compression: "uncompressed",
      compat_level: nil,
      storage_options: nil,
      credential_provider: "auto",
      retries: 2
    )
      return_bytes = file.nil?
      target = nil
      if file.nil?
        target = StringIO.new
        target.set_encoding(Encoding::BINARY)
      else
        target = file
      end

      lazy.sink_ipc(
        target,
        compression: compression,
        compat_level: compat_level,
        storage_options: storage_options,
        credential_provider: credential_provider,
        retries: retries
      )
      return_bytes ? target.string : nil
    end

    # Write to Arrow IPC record batch stream.
    #
    # See "Streaming format" in https://arrow.apache.org/docs/python/ipc.html.
    #
    # @param file [Object]
    #   Path or writable file-like object to which the IPC record batch data will
    #   be written. If set to `nil`, the output is returned as a BytesIO object.
    # @param compression ['uncompressed', 'lz4', 'zstd']
    #   Compression method. Defaults to "uncompressed".
    # @param compat_level [Object]
    #   Use a specific compatibility level
    #   when exporting Polars' internal data structures.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.write_ipc_stream("new_file.arrow")
    def write_ipc_stream(
      file,
      compression: "uncompressed",
      compat_level: nil
    )
      return_bytes = file.nil?
      if return_bytes
        file = StringIO.new
        file.set_encoding(Encoding::BINARY)
      elsif Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end

      if compat_level.nil?
        compat_level = true
      end

      if compression.nil?
        compression = "uncompressed"
      end

      _df.write_ipc_stream(file, compression, compat_level)
      return_bytes ? file.string : nil
    end

    # Write to Apache Parquet file.
    #
    # @param file [String, Pathname, StringIO]
    #   File path to which the file should be written.
    # @param compression ["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"]
    #   Choose "zstd" for good compression performance.
    #   Choose "lz4" for fast compression/decompression.
    #   Choose "snappy" for more backwards compatibility guarantees
    #   when you deal with older parquet readers.
    # @param compression_level [Integer, nil]
    #   The level of compression to use. Higher compression means smaller files on
    #   disk.
    #
    #   - "gzip" : min-level: 0, max-level: 10.
    #   - "brotli" : min-level: 0, max-level: 11.
    #   - "zstd" : min-level: 1, max-level: 22.
    # @param statistics [Boolean]
    #   Write statistics to the parquet headers. This requires extra compute.
    # @param row_group_size [Integer, nil]
    #   Size of the row groups in number of rows. Defaults to 512^2 rows.
    # @param data_page_size [Integer, nil]
    #   Size of the data page in bytes. Defaults to 1024^2 bytes.
    #
    # @return [nil]
    def write_parquet(
      file,
      compression: "zstd",
      compression_level: nil,
      statistics: false,
      row_group_size: nil,
      data_page_size: nil,
      partition_by: nil,
      partition_chunk_size_bytes: 4_294_967_296,
      storage_options: nil,
      credential_provider: "auto",
      retries: 2,
      metadata: nil,
      mkdir: false
    )
      if compression.nil?
        compression = "uncompressed"
      end
      if Utils.pathlike?(file)
        file = Utils.normalize_filepath(file)
      end

      target = file
      if !partition_by.nil?
        raise Todo
      end

      lazy.sink_parquet(
        target,
        compression: compression,
        compression_level: compression_level,
        statistics: statistics,
        row_group_size: row_group_size,
        data_page_size: data_page_size,
        storage_options: storage_options,
        credential_provider: credential_provider,
        retries: retries,
        metadata: metadata,
        mkdir: mkdir
      )
    end

    # Write the data in a Polars DataFrame to a database.
    #
    # @param table_name [String]
    #   Schema-qualified name of the table to create or append to in the target
    #   SQL database.
    # @param connection [Object]
    #   An existing Active Record connection against the target database.
    # @param if_table_exists ['append', 'replace', 'fail']
    #   The insert mode:
    #
    #   * 'replace' will create a new database table, overwriting an existing one.
    #   * 'append' will append to an existing table.
    #   * 'fail' will fail if table already exists.
    #
    # @return [Integer]
    #
    # @note
    #   This functionality is experimental. It may be changed at any point without it being considered a breaking change.
    def write_database(table_name, connection = nil, if_table_exists: "fail")
      if !defined?(ActiveRecord)
        raise Error, "Active Record not available"
      elsif ActiveRecord::VERSION::MAJOR < 7
        raise Error, "Requires Active Record 7+"
      end

      valid_write_modes = ["append", "replace", "fail"]
      if !valid_write_modes.include?(if_table_exists)
        msg = "write_database `if_table_exists` must be one of #{valid_write_modes.inspect}, got #{if_table_exists.inspect}"
        raise ArgumentError, msg
      end

      with_connection(connection) do |connection|
        table_exists = connection.table_exists?(table_name)
        if table_exists && if_table_exists == "fail"
          raise ArgumentError, "Table already exists"
        end

        create_table = !table_exists || if_table_exists == "replace"
        maybe_transaction(connection, create_table) do
          if create_table
            mysql = connection.adapter_name.match?(/mysql|trilogy/i)
            force = if_table_exists == "replace"
            connection.create_table(table_name, id: false, force: force) do |t|
              schema.each do |c, dtype|
                options = {}
                column_type =
                  case dtype
                  when Binary
                    :binary
                  when Boolean
                    :boolean
                  when Date
                    :date
                  when Datetime
                    :datetime
                  when Decimal
                    if mysql
                      options[:precision] = dtype.precision || 65
                      options[:scale] = dtype.scale || 30
                    end
                    :decimal
                  when Float32
                    options[:limit] = 24
                    :float
                  when Float64
                    options[:limit] = 53
                    :float
                  when Int8
                    options[:limit] = 1
                    :integer
                  when Int16
                    options[:limit] = 2
                    :integer
                  when Int32
                    options[:limit] = 4
                    :integer
                  when Int64
                    options[:limit] = 8
                    :integer
                  when UInt8
                    if mysql
                      options[:limit] = 1
                      options[:unsigned] = true
                    else
                      options[:limit] = 2
                    end
                    :integer
                  when UInt16
                    if mysql
                      options[:limit] = 2
                      options[:unsigned] = true
                    else
                      options[:limit] = 4
                    end
                    :integer
                  when UInt32
                    if mysql
                      options[:limit] = 4
                      options[:unsigned] = true
                    else
                      options[:limit] = 8
                    end
                    :integer
                  when UInt64
                    if mysql
                      options[:limit] = 8
                      options[:unsigned] = true
                      :integer
                    else
                      options[:precision] = 20
                      options[:scale] = 0
                      :decimal
                    end
                  when String
                    :text
                  when Time
                    :time
                  else
                    raise ArgumentError, "column type not supported yet: #{dtype}"
                  end
                t.column c, column_type, **options
              end
            end
          end

          quoted_table = connection.quote_table_name(table_name)
          quoted_columns = columns.map { |c| connection.quote_column_name(c) }
          rows = cast({Polars::UInt64 => Polars::String}).rows(named: false).map { |row| "(#{row.map { |v| connection.quote(v) }.join(", ")})" }
          connection.exec_update("INSERT INTO #{quoted_table} (#{quoted_columns.join(", ")}) VALUES #{rows.join(", ")}")
        end
      end
    end

    # Write DataFrame to an Iceberg table.
    #
    # @note
    #   This functionality is currently considered **unstable**. It may be
    #   changed at any point without it being considered a breaking change.
    #
    # @param target [Object]
    #   Name of the table or the Table object representing an Iceberg table.
    # @param mode ['append', 'overwrite']
    #   How to handle existing data.
    #
    #   - If 'append', will add new data.
    #   - If 'overwrite', will replace table with new data.
    #
    # @return [nil]
    def write_iceberg(target, mode:)
      require "iceberg"

      table =
        if target.is_a?(Iceberg::Table)
          target
        else
          raise Todo
        end

      data = self

      if mode == "append"
        table.append(data)
      else
        raise Todo
      end
    end

    # Write DataFrame as delta table.
    #
    # @param target [Object]
    #   URI of a table or a DeltaTable object.
    # @param mode ["error", "append", "overwrite", "ignore", "merge"]
    #   How to handle existing data.
    # @param storage_options [Hash]
    #   Extra options for the storage backends supported by `deltalake-rb`.
    # @param delta_write_options [Hash]
    #   Additional keyword arguments while writing a Delta lake Table.
    # @param delta_merge_options [Hash]
    #   Keyword arguments which are required to `MERGE` a Delta lake Table.
    #
    # @return [nil]
    def write_delta(
      target,
      mode: "error",
      storage_options: nil,
      delta_write_options: nil,
      delta_merge_options: nil
    )
      Polars.send(:_check_if_delta_available)

      if Utils.pathlike?(target)
        target = Polars.send(:_resolve_delta_lake_uri, target.to_s, strict: false)
      end

      data = self

      if mode == "merge"
        if delta_merge_options.nil?
          msg = "You need to pass delta_merge_options with at least a given predicate for `MERGE` to work."
          raise ArgumentError, msg
        end
        if target.is_a?(::String)
          dt = DeltaLake::Table.new(target, storage_options: storage_options)
        else
          dt = target
        end

        predicate = delta_merge_options.delete(:predicate)
        dt.merge(data, predicate, **delta_merge_options)
      else
        delta_write_options ||= {}

        DeltaLake.write(
          target,
          data,
          mode: mode,
          storage_options: storage_options,
          **delta_write_options
        )
      end
    end

    # Return an estimation of the total (heap) allocated size of the DataFrame.
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "x" => 1_000_000.times.to_a.reverse,
    #       "y" => 1_000_000.times.map { |v| v / 1000.0 },
    #       "z" => 1_000_000.times.map(&:to_s)
    #     },
    #     schema: {"x" => :u32, "y" => :f64, "z" => :str}
    #   )
    #   df.estimated_size
    #   # => 25888898
    #   df.estimated_size("mb")
    #   # => 17.0601749420166
    def estimated_size(unit = "b")
      sz = _df.estimated_size
      Utils.scale_bytes(sz, to: unit)
    end

    # Transpose a DataFrame over the diagonal.
    #
    # @param include_header [Boolean]
    #   If set, the column names will be added as first column.
    # @param header_name [String]
    #   If `include_header` is set, this determines the name of the column that will
    #   be inserted.
    # @param column_names [Array]
    #   Optional generator/iterator that yields column names. Will be used to
    #   replace the columns in the DataFrame.
    #
    # @return [DataFrame]
    #
    # @note
    #   This is a very expensive operation. Perhaps you can do it differently.
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3], "b" => [1, 2, 3]})
    #   df.transpose(include_header: true)
    #   # =>
    #   # shape: (2, 4)
    #   # ┌────────┬──────────┬──────────┬──────────┐
    #   # │ column ┆ column_0 ┆ column_1 ┆ column_2 │
    #   # │ ---    ┆ ---      ┆ ---      ┆ ---      │
    #   # │ str    ┆ i64      ┆ i64      ┆ i64      │
    #   # ╞════════╪══════════╪══════════╪══════════╡
    #   # │ a      ┆ 1        ┆ 2        ┆ 3        │
    #   # │ b      ┆ 1        ┆ 2        ┆ 3        │
    #   # └────────┴──────────┴──────────┴──────────┘
    #
    # @example Replace the auto-generated column names with a list
    #   df.transpose(include_header: false, column_names: ["a", "b", "c"])
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 2   ┆ 3   │
    #   # │ 1   ┆ 2   ┆ 3   │
    #   # └─────┴─────┴─────┘
    #
    # @example Include the header as a separate column
    #   df.transpose(
    #     include_header: true, header_name: "foo", column_names: ["a", "b", "c"]
    #   )
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬─────┐
    #   # │ foo ┆ a   ┆ b   ┆ c   │
    #   # │ --- ┆ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╪═════╡
    #   # │ a   ┆ 1   ┆ 2   ┆ 3   │
    #   # │ b   ┆ 1   ┆ 2   ┆ 3   │
    #   # └─────┴─────┴─────┴─────┘
    def transpose(include_header: false, header_name: "column", column_names: nil)
      keep_names_as = include_header ? header_name : nil
      _from_rbdf(_df.transpose(keep_names_as, column_names))
    end

    # Reverse the DataFrame.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "key" => ["a", "b", "c"],
    #       "val" => [1, 2, 3]
    #     }
    #   )
    #   df.reverse
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ key ┆ val │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ c   ┆ 3   │
    #   # │ b   ┆ 2   │
    #   # │ a   ┆ 1   │
    #   # └─────┴─────┘
    def reverse
      select(Polars.col("*").reverse)
    end

    # Rename column names.
    #
    # @param mapping [Hash]
    #   Key value pairs that map from old name to new name.
    # @param strict [Boolean]
    #   Validate that all column names exist in the current schema,
    #   and throw an exception if any do not. (Note that this parameter
    #   is a no-op when passing a function to `mapping`).
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.rename({"foo" => "apple"})
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ apple ┆ bar ┆ ham │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ i64   ┆ i64 ┆ str │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 1     ┆ 6   ┆ a   │
    #   # │ 2     ┆ 7   ┆ b   │
    #   # │ 3     ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    def rename(mapping, strict: true)
      lazy.rename(mapping, strict: strict).collect(no_optimization: true)
    end

    # Insert a Series at a certain column index. This operation is in place.
    #
    # @param index [Integer]
    #   Column to insert the new `Series` column.
    # @param column [Series]
    #   `Series` to insert.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   s = Polars::Series.new("baz", [97, 98, 99])
    #   df.insert_column(1, s)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ baz ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 97  ┆ 4   │
    #   # │ 2   ┆ 98  ┆ 5   │
    #   # │ 3   ┆ 99  ┆ 6   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   s = Polars::Series.new("d", [-2.5, 15, 20.5, 0])
    #   df.insert_column(3, s)
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬──────┬───────┬──────┐
    #   # │ a   ┆ b    ┆ c     ┆ d    │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  │
    #   # ╞═════╪══════╪═══════╪══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ -2.5 │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 15.0 │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 20.5 │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 0.0  │
    #   # └─────┴──────┴───────┴──────┘
    def insert_column(index, column)
      if index < 0
        index = width + index
      end
      _df.insert_column(index, column._s)
      self
    end

    # Filter the rows in the DataFrame based on a predicate expression.
    #
    # @param predicates [Array]
    #   Expression(s) that evaluate to a boolean Series.
    # @param constraints [Hash]
    #   Column filters; use `name = value` to filter columns by the supplied value.
    #   Each constraint will behave the same as `Polars.col(name).eq(value)`, and
    #   be implicitly joined with the other filter conditions using `&`.
    #
    # @return [DataFrame]
    #
    # @example Filter on one condition:
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.filter(Polars.col("foo") < 3)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example Filter on multiple conditions:
    #   df.filter((Polars.col("foo") < 3) & (Polars.col("ham") == "a"))
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # └─────┴─────┴─────┘
    def filter(*predicates, **constraints)
      lazy.filter(*predicates, **constraints).collect(_eager: true)
    end

    # Remove rows, dropping those that match the given predicate expression(s).
    #
    # The original order of the remaining rows is preserved.
    #
    # Rows where the filter predicate does not evaluate to True are retained
    # (this includes rows where the predicate evaluates as `null`).
    #
    # @param predicates [Array]
    #   Expression that evaluates to a boolean Series.
    # @param constraints [Hash]
    #   Column filters; use `name = value` to filter columns using the supplied
    #   value. Each constraint behaves the same as `Polars.col(name).eq(value)`,
    #   and is implicitly joined with the other filter conditions using `&`.
    #
    # @return [DataFrame]
    #
    # @example Remove rows matching a condition:
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [2, 3, nil, 4, 0],
    #       "bar" => [5, 6, nil, nil, 0],
    #       "ham" => ["a", "b", nil, "c", "d"]
    #     }
    #   )
    #   df.remove(Polars.col("bar") >= 5)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ 4    ┆ null ┆ c    │
    #   # │ 0    ┆ 0    ┆ d    │
    #   # └──────┴──────┴──────┘
    #
    # @example Discard rows based on multiple conditions, combined with and/or operators:
    #   df.remove(
    #     (Polars.col("foo") >= 0) & (Polars.col("bar") >= 0),
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ 4    ┆ null ┆ c    │
    #   # └──────┴──────┴──────┘
    #
    # @example
    #   df.remove(
    #     (Polars.col("foo") >= 0) | (Polars.col("bar") >= 0),
    #   )
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # └──────┴──────┴──────┘
    #
    # @example Provide multiple constraints using `*args` syntax:
    #   df.remove(
    #     Polars.col("ham").is_not_null,
    #     Polars.col("bar") >= 0
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ 4    ┆ null ┆ c    │
    #   # └──────┴──────┴──────┘
    #
    # @example Provide constraints(s) using `**kwargs` syntax:
    #   df.remove(foo: 0, bar: 0)
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ 2    ┆ 5    ┆ a    │
    #   # │ 3    ┆ 6    ┆ b    │
    #   # │ null ┆ null ┆ null │
    #   # │ 4    ┆ null ┆ c    │
    #   # └──────┴──────┴──────┘
    #
    # @example Remove rows by comparing two columns against each other:
    #   df.remove(
    #     Polars.col("foo").ne_missing(Polars.col("bar"))
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ 0    ┆ 0    ┆ d    │
    #   # └──────┴──────┴──────┘
    def remove(
      *predicates,
      **constraints
    )
      lazy
      .remove(*predicates, **constraints)
      .collect(_eager: true)
    end

    # Summary statistics for a DataFrame.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.0, 2.8, 3.0],
    #       "b" => [4, 5, nil],
    #       "c" => [true, false, true],
    #       "d" => [nil, "b", "c"],
    #       "e" => ["usd", "eur", nil]
    #     }
    #   )
    #   df.describe
    #   # =>
    #   # shape: (7, 6)
    #   # ┌────────────┬──────────┬──────────┬──────────┬──────┬──────┐
    #   # │ describe   ┆ a        ┆ b        ┆ c        ┆ d    ┆ e    │
    #   # │ ---        ┆ ---      ┆ ---      ┆ ---      ┆ ---  ┆ ---  │
    #   # │ str        ┆ f64      ┆ f64      ┆ f64      ┆ str  ┆ str  │
    #   # ╞════════════╪══════════╪══════════╪══════════╪══════╪══════╡
    #   # │ count      ┆ 3.0      ┆ 3.0      ┆ 3.0      ┆ 3    ┆ 3    │
    #   # │ null_count ┆ 0.0      ┆ 1.0      ┆ 0.0      ┆ 1    ┆ 1    │
    #   # │ mean       ┆ 2.266667 ┆ 4.5      ┆ 0.666667 ┆ null ┆ null │
    #   # │ std        ┆ 1.101514 ┆ 0.707107 ┆ 0.57735  ┆ null ┆ null │
    #   # │ min        ┆ 1.0      ┆ 4.0      ┆ 0.0      ┆ b    ┆ eur  │
    #   # │ max        ┆ 3.0      ┆ 5.0      ┆ 1.0      ┆ c    ┆ usd  │
    #   # │ median     ┆ 2.8      ┆ 4.5      ┆ 1.0      ┆ null ┆ null │
    #   # └────────────┴──────────┴──────────┴──────────┴──────┴──────┘
    def describe
      describe_cast = lambda do |stat|
        columns = []
        self.columns.each_with_index do |s, i|
          if self[s].dtype.numeric? || self[s].dtype == Boolean
            columns << stat[0.., i].cast(:f64)
          else
            # for dates, strings, etc, we cast to string so that all
            # statistics can be shown
            columns << stat[0.., i].cast(:str)
          end
        end
        self.class.new(columns)
      end

      summary = _from_rbdf(
        Polars.concat(
          [
            describe_cast.(
              self.class.new(columns.to_h { |c| [c, [height]] })
            ),
            describe_cast.(null_count),
            describe_cast.(mean),
            describe_cast.(std),
            describe_cast.(min),
            describe_cast.(max),
            describe_cast.(median)
          ]
        )._df
      )
      summary.insert_column(
        0,
        Polars::Series.new(
          "describe",
          ["count", "null_count", "mean", "std", "min", "max", "median"],
        )
      )
      summary
    end

    # Find the index of a column by name.
    #
    # @param name [String]
    #   Name of the column to find.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => [1, 2, 3], "bar" => [6, 7, 8], "ham" => ["a", "b", "c"]}
    #   )
    #   df.get_column_index("ham")
    #   # => 2
    def get_column_index(name)
      _df.get_column_index(name)
    end

    # Replace a column at an index location.
    #
    # @param index [Integer]
    #   Column index.
    # @param column [Series]
    #   Series that will replace the column.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   s = Polars::Series.new("apple", [10, 20, 30])
    #   df.replace_column(0, s)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ apple ┆ bar ┆ ham │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ i64   ┆ i64 ┆ str │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 10    ┆ 6   ┆ a   │
    #   # │ 20    ┆ 7   ┆ b   │
    #   # │ 30    ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    def replace_column(index, column)
      if index < 0
        index = width + index
      end
      _df.replace_column(index, column._s)
      self
    end

    # Sort the dataframe by the given columns.
    #
    # @param by [Object]
    #   Column(s) to sort by. Accepts expression input, including selectors. Strings
    #   are parsed as column names.
    # @param more_by [Array]
    #   Additional columns to sort by, specified as positional arguments.
    # @param descending [Boolean]
    #   Sort in descending order. When sorting by multiple columns, can be specified
    #   per column by passing a sequence of booleans.
    # @param nulls_last [Boolean]
    #   Place null values last; can specify a single boolean applying to all columns
    #   or a sequence of booleans for per-column control.
    # @param multithreaded [Boolean]
    #   Sort using multiple threads.
    # @param maintain_order [Boolean]
    #   Whether the order should be maintained if elements are equal.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.sort("foo", descending: true)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    #
    # @example Sort by multiple columns.
    #   df.sort(
    #     [Polars.col("foo"), Polars.col("bar")**2],
    #     descending: [true, false]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    def sort(
      by,
      *more_by,
      descending: false,
      nulls_last: false,
      multithreaded: true,
      maintain_order: false
    )
      lazy
        .sort(
          by,
          *more_by,
          descending: descending,
          nulls_last: nulls_last,
          multithreaded: multithreaded,
          maintain_order: maintain_order
        )
        .collect(no_optimization: true)
    end

    # Sort the DataFrame by column in-place.
    #
    # @param by [String]
    #   By which column to sort.
    # @param descending [Boolean]
    #   Reverse/descending sort.
    # @param nulls_last [Boolean]
    #   Place null values last. Can only be used if sorted by a single column.
    #
    # @return [DataFrame]
    def sort!(by, descending: false, nulls_last: false)
      self._df = sort(by, descending: descending, nulls_last: nulls_last)._df
    end

    # Execute a SQL query against the DataFrame.
    #
    # @note
    #   This functionality is considered **unstable**, although it is close to
    #   being considered stable. It may be changed at any point without it being
    #   considered a breaking change.
    #
    # @param query [String]
    #   SQL query to execute.
    # @param table_name [String]
    #   Optionally provide an explicit name for the table that represents the
    #   calling frame (defaults to "self").
    #
    # @return [DataFrame]
    #
    # @note
    #   * The calling frame is automatically registered as a table in the SQL context
    #     under the name "self". If you want access to the DataFrames and LazyFrames
    #     found in the current globals, use the top-level :meth:`pl.sql <polars.sql>`.
    #   * More control over registration and execution behaviour is available by
    #     using the :class:`SQLContext` object.
    #   * The SQL query executes in lazy mode before being collected and returned
    #     as a DataFrame.
    #
    # @example Query the DataFrame using SQL:
    #   df1 = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["zz", "yy", "xx"],
    #       "c" => [Date.new(1999, 12, 31), Date.new(2010, 10, 10), Date.new(2077, 8, 8)]
    #     }
    #   )
    #   df1.sql("SELECT c, b FROM self WHERE a > 1")
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬─────┐
    #   # │ c          ┆ b   │
    #   # │ ---        ┆ --- │
    #   # │ date       ┆ str │
    #   # ╞════════════╪═════╡
    #   # │ 2010-10-10 ┆ yy  │
    #   # │ 2077-08-08 ┆ xx  │
    #   # └────────────┴─────┘
    #
    # @example Apply transformations to a DataFrame using SQL, aliasing "self" to "frame".
    #   df1.sql(
    #     "
    #       SELECT
    #           a,
    #           (a % 2 == 0) AS a_is_even,
    #           CONCAT_WS(':', b, b) AS b_b,
    #           EXTRACT(year FROM c) AS year,
    #           0::float4 AS \"zero\",
    #       FROM frame
    #     ",
    #     table_name: "frame"
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌─────┬───────────┬───────┬──────┬──────┐
    #   # │ a   ┆ a_is_even ┆ b_b   ┆ year ┆ zero │
    #   # │ --- ┆ ---       ┆ ---   ┆ ---  ┆ ---  │
    #   # │ i64 ┆ bool      ┆ str   ┆ i32  ┆ f32  │
    #   # ╞═════╪═══════════╪═══════╪══════╪══════╡
    #   # │ 1   ┆ false     ┆ zz:zz ┆ 1999 ┆ 0.0  │
    #   # │ 2   ┆ true      ┆ yy:yy ┆ 2010 ┆ 0.0  │
    #   # │ 3   ┆ false     ┆ xx:xx ┆ 2077 ┆ 0.0  │
    #   # └─────┴───────────┴───────┴──────┴──────┘
    def sql(query, table_name: "self")
      ctx = SQLContext.new(eager_execution: true)
      name = table_name || "self"
      ctx.register(name, self)
      ctx.execute(query)
    end

    # Return the `k` largest rows.
    #
    # Non-null elements are always preferred over null elements, regardless of
    # the value of `reverse`. The output is not guaranteed to be in any
    # particular order, call `sort` after this function if you wish the
    # output to be sorted.
    #
    # @param k [Integer]
    #   Number of rows to return.
    # @param by [Object]
    #   Column(s) used to determine the top rows.
    #   Accepts expression input. Strings are parsed as column names.
    # @param reverse [Object]
    #   Consider the `k` smallest elements of the `by` column(s) (instead of the `k`
    #   largest). This can be specified per column by passing a sequence of
    #   booleans.
    #
    # @return [DataFrame]
    #
    # @example Get the rows which contain the 4 largest values in column b.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [2, 1, 1, 3, 2, 1]
    #     }
    #   )
    #   df.top_k(4, by: "b")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ b   ┆ 3   │
    #   # │ a   ┆ 2   │
    #   # │ b   ┆ 2   │
    #   # │ b   ┆ 1   │
    #   # └─────┴─────┘
    #
    # @example Get the rows which contain the 4 largest values when sorting on column b and a.
    #   df.top_k(4, by: ["b", "a"])
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ b   ┆ 3   │
    #   # │ b   ┆ 2   │
    #   # │ a   ┆ 2   │
    #   # │ c   ┆ 1   │
    #   # └─────┴─────┘
    def top_k(
      k,
      by:,
      reverse: false
    )
      lazy
      .top_k(k, by: by, reverse: reverse)
      .collect(
        # optimizations=QueryOptFlags(
        #   projection_pushdown=False,
        #   predicate_pushdown=False,
        #   comm_subplan_elim=False,
        #   slice_pushdown=True
        # )
      )
    end

    # Return the `k` smallest rows.
    #
    # Non-null elements are always preferred over null elements, regardless of
    # the value of `reverse`. The output is not guaranteed to be in any
    # particular order, call `sort` after this function if you wish the
    # output to be sorted.
    #
    # @param k [Integer]
    #   Number of rows to return.
    # @param by [Object]
    #   Column(s) used to determine the bottom rows.
    #   Accepts expression input. Strings are parsed as column names.
    # @param reverse [Object]
    #   Consider the `k` largest elements of the `by` column(s) (instead of the `k`
    #   smallest). This can be specified per column by passing a sequence of
    #   booleans.
    #
    # @return [DataFrame]
    #
    # @example Get the rows which contain the 4 smallest values in column b.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [2, 1, 1, 3, 2, 1]
    #     }
    #   )
    #   df.bottom_k(4, by: "b")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ b   ┆ 1   │
    #   # │ a   ┆ 1   │
    #   # │ c   ┆ 1   │
    #   # │ a   ┆ 2   │
    #   # └─────┴─────┘
    #
    # @example Get the rows which contain the 4 smallest values when sorting on column a and b.
    #   df.bottom_k(4, by: ["a", "b"])
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 1   │
    #   # │ a   ┆ 2   │
    #   # │ b   ┆ 1   │
    #   # │ b   ┆ 2   │
    #   # └─────┴─────┘
    def bottom_k(
      k,
      by:,
      reverse: false
    )
      lazy
      .bottom_k(k, by: by, reverse: reverse)
      .collect(
        # optimizations=QueryOptFlags(
        #   projection_pushdown=False,
        #   predicate_pushdown=False,
        #   comm_subplan_elim=False,
        #   slice_pushdown=True,
        # )
      )
    end

    # Check if DataFrame is equal to other.
    #
    # @param other [DataFrame]
    #   DataFrame to compare with.
    # @param null_equal [Boolean]
    #   Consider null values as equal.
    #
    # @return [Boolean]
    #
    # @example
    #   df1 = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df2 = Polars::DataFrame.new(
    #     {
    #       "foo" => [3, 2, 1],
    #       "bar" => [8.0, 7.0, 6.0],
    #       "ham" => ["c", "b", "a"]
    #     }
    #   )
    #   df1.equals(df1)
    #   # => true
    #   df1.equals(df2)
    #   # => false
    def equals(other, null_equal: true)
      _df.equals(other._df, null_equal)
    end

    # Get a slice of this DataFrame.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer, nil]
    #   Length of the slice. If set to `nil`, all rows starting at the offset
    #   will be selected.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.slice(1, 2)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # └─────┴─────┴─────┘
    def slice(offset, length = nil)
      if !length.nil? && length < 0
        length = height - offset + length
      end
      _from_rbdf(_df.slice(offset, length))
    end

    # Get the first `n` rows.
    #
    # Alias for {#head}.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"foo" => [1, 2, 3, 4, 5, 6], "bar" => ["a", "b", "c", "d", "e", "f"]}
    #   )
    #   df.limit(4)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ str │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ a   │
    #   # │ 2   ┆ b   │
    #   # │ 3   ┆ c   │
    #   # │ 4   ┆ d   │
    #   # └─────┴─────┘
    def limit(n = 5)
      head(n)
    end

    # Get the first `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.head(3)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def head(n = 5)
      _from_rbdf(_df.head(n))
    end

    # Get the last `n` rows.
    #
    # @param n [Integer]
    #   Number of rows to return.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3, 4, 5],
    #       "bar" => [6, 7, 8, 9, 10],
    #       "ham" => ["a", "b", "c", "d", "e"]
    #     }
    #   )
    #   df.tail(3)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8   ┆ c   │
    #   # │ 4   ┆ 9   ┆ d   │
    #   # │ 5   ┆ 10  ┆ e   │
    #   # └─────┴─────┴─────┘
    def tail(n = 5)
      _from_rbdf(_df.tail(n))
    end

    # Drop all rows that contain one or more NaN values.
    #
    # The original order of the remaining rows is preserved.
    #
    # @param subset [Object]
    #   Column name(s) for which NaN values are considered; if set to `nil`
    #   (default), use all columns (note that only floating-point columns
    #   can contain NaNs).
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [-20.5, Float::NAN, 80.0],
    #       "bar" => [Float::NAN, 110.0, 25.5],
    #       "ham" => ["xxx", "yyy", nil]
    #     }
    #   )
    #   df.drop_nans
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ f64  ┆ f64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ 80.0 ┆ 25.5 ┆ null │
    #   # └──────┴──────┴──────┘
    #
    # @example
    #   df.drop_nans(subset: ["bar"])
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬───────┬──────┐
    #   # │ foo  ┆ bar   ┆ ham  │
    #   # │ ---  ┆ ---   ┆ ---  │
    #   # │ f64  ┆ f64   ┆ str  │
    #   # ╞══════╪═══════╪══════╡
    #   # │ NaN  ┆ 110.0 ┆ yyy  │
    #   # │ 80.0 ┆ 25.5  ┆ null │
    #   # └──────┴───────┴──────┘
    def drop_nans(subset: nil)
      lazy.drop_nans(subset: subset).collect(_eager: true)
    end

    # Drop all rows that contain one or more null values.
    #
    # The original order of the remaining rows is preserved.
    #
    # @param subset [Object]
    #   Column name(s) for which null values are considered.
    #   If set to `nil` (default), use all columns.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, nil, 8],
    #       "ham" => ["a", "b", nil]
    #     }
    #   )
    #   df.drop_nulls
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   df.drop_nulls(subset: Polars.cs.integer)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1   ┆ 6   ┆ a    │
    #   # │ 3   ┆ 8   ┆ null │
    #   # └─────┴─────┴──────┘
    def drop_nulls(subset: nil)
      lazy.drop_nulls(subset: subset).collect(_eager: true)
    end

    # Offers a structured way to apply a sequence of user-defined functions (UDFs).
    #
    # @param function [Object]
    #   Callable; will receive the frame as the first parameter,
    #   followed by any given args/kwargs.
    # @param args [Object]
    #   Arguments to pass to the UDF.
    # @param kwargs [Object]
    #   Keyword arguments to pass to the UDF.
    #
    # @return [Object]
    #
    # @note
    #   It is recommended to use LazyFrame when piping operations, in order
    #   to fully take advantage of query optimization and parallelization.
    #   See {#lazy}.
    #
    # @example
    #   cast_str_to_int = lambda do |data, col_name:|
    #     data.with_columns(Polars.col(col_name).cast(:i64))
    #   end
    #
    #   df = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => ["10", "20", "30", "40"]})
    #   df.pipe(cast_str_to_int, col_name: "b")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 10  │
    #   # │ 2   ┆ 20  │
    #   # │ 3   ┆ 30  │
    #   # │ 4   ┆ 40  │
    #   # └─────┴─────┘
    def pipe(function, *args, **kwargs, &block)
      function.call(self, *args, **kwargs, &block)
    end

    # Add a column at index 0 that counts the rows.
    #
    # @param name [String]
    #   Name of the column to add.
    # @param offset [Integer]
    #   Start the row count at this offset.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.with_row_index
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ index ┆ a   ┆ b   │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ u32   ┆ i64 ┆ i64 │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 0     ┆ 1   ┆ 2   │
    #   # │ 1     ┆ 3   ┆ 4   │
    #   # │ 2     ┆ 5   ┆ 6   │
    #   # └───────┴─────┴─────┘
    def with_row_index(name: "index", offset: 0)
      _from_rbdf(_df.with_row_index(name, offset))
    end
    alias_method :with_row_count, :with_row_index

    # Start a group by operation.
    #
    # @param by [Object]
    #   Column(s) to group by.
    # @param maintain_order [Boolean]
    #   Make sure that the order of the groups remain consistent. This is more
    #   expensive than a default group by. Note that this only works in expression
    #   aggregations.
    # @param named_by [Hash]
    #   Additional columns to group by, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [GroupBy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["a", "b", "a", "b", "b", "c"],
    #       "b" => [1, 2, 3, 4, 5, 6],
    #       "c" => [6, 5, 4, 3, 2, 1]
    #     }
    #   )
    #   df.group_by("a").agg(Polars.col("b").sum).sort("a")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 4   │
    #   # │ b   ┆ 11  │
    #   # │ c   ┆ 6   │
    #   # └─────┴─────┘
    def group_by(by, maintain_order: false, **named_by)
      named_by.each do |_, value|
        if !(value.is_a?(::String) || value.is_a?(Expr) || value.is_a?(Series))
          msg = "Expected Polars expression or object convertible to one, got #{value.class.name}."
          raise TypeError, msg
        end
      end
      GroupBy.new(
        self,
        by,
        **named_by,
        maintain_order: maintain_order
      )
    end
    alias_method :group, :group_by

    # Create rolling groups based on a time column.
    #
    # Also works for index values of type `:i32` or `:i64`.
    #
    # Different from a `dynamic_group_by` the windows are now determined by the
    # individual values and are not of constant intervals. For constant intervals use
    # *group_by_dynamic*
    #
    # The `period` and `offset` arguments are created either from a timedelta, or
    # by using the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # In case of a group_by_rolling on an integer column, the windows are defined by:
    #
    # - **"1i"      # length 1**
    # - **"10i"     # length 10**
    #
    # @param index_column [Object]
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a rolling group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param period [Object]
    #   Length of the window.
    # @param offset [Object]
    #   Offset of the window. Default is -period.
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param group_by [Object]
    #   Also group by this column/these columns.
    #
    # @return [RollingGroupBy]
    #
    # @example
    #   dates = [
    #     "2020-01-01 13:45:48",
    #     "2020-01-01 16:42:13",
    #     "2020-01-01 16:45:09",
    #     "2020-01-02 18:12:48",
    #     "2020-01-03 19:45:32",
    #     "2020-01-08 23:16:43"
    #   ]
    #   df = Polars::DataFrame.new({"dt" => dates, "a" => [3, 7, 5, 9, 2, 1]}).with_columns(
    #     Polars.col("dt").str.strptime(Polars::Datetime).set_sorted
    #   )
    #   df.rolling(index_column: "dt", period: "2d").agg(
    #     [
    #       Polars.sum("a").alias("sum_a"),
    #       Polars.min("a").alias("min_a"),
    #       Polars.max("a").alias("max_a")
    #     ]
    #   )
    #   # =>
    #   # shape: (6, 4)
    #   # ┌─────────────────────┬───────┬───────┬───────┐
    #   # │ dt                  ┆ sum_a ┆ min_a ┆ max_a │
    #   # │ ---                 ┆ ---   ┆ ---   ┆ ---   │
    #   # │ datetime[μs]        ┆ i64   ┆ i64   ┆ i64   │
    #   # ╞═════════════════════╪═══════╪═══════╪═══════╡
    #   # │ 2020-01-01 13:45:48 ┆ 3     ┆ 3     ┆ 3     │
    #   # │ 2020-01-01 16:42:13 ┆ 10    ┆ 3     ┆ 7     │
    #   # │ 2020-01-01 16:45:09 ┆ 15    ┆ 3     ┆ 7     │
    #   # │ 2020-01-02 18:12:48 ┆ 24    ┆ 3     ┆ 9     │
    #   # │ 2020-01-03 19:45:32 ┆ 11    ┆ 2     ┆ 9     │
    #   # │ 2020-01-08 23:16:43 ┆ 1     ┆ 1     ┆ 1     │
    #   # └─────────────────────┴───────┴───────┴───────┘
    def rolling(
      index_column:,
      period:,
      offset: nil,
      closed: "right",
      group_by: nil
    )
      RollingGroupBy.new(self, index_column, period, offset, closed, group_by)
    end

    # Group based on a time value (or index value of type `:i32`, `:i64`).
    #
    # Time windows are calculated and rows are assigned to windows. Different from a
    # normal group by is that a row can be member of multiple groups. The time/index
    # window could be seen as a rolling window, with a window size determined by
    # dates/times/values instead of slots in the DataFrame.
    #
    # A window is defined by:
    #
    # - every: interval of the window
    # - period: length of the window
    # - offset: offset of the window
    #
    # The `every`, `period` and `offset` arguments are created with
    # the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # In case of a group_by_dynamic on an integer column, the windows are defined by:
    #
    # - "1i"      # length 1
    # - "10i"     # length 10
    #
    # @param index_column
    #   Column used to group based on the time window.
    #   Often to type Date/Datetime
    #   This column must be sorted in ascending order. If not the output will not
    #   make sense.
    #
    #   In case of a dynamic group by on indices, dtype needs to be one of
    #   `:i32`, `:i64`. Note that `:i32` gets temporarily cast to `:i64`, so if
    #   performance matters use an `:i64` column.
    # @param every
    #   Interval of the window.
    # @param period
    #   Length of the window, if nil it is equal to 'every'.
    # @param offset
    #   Offset of the window if nil and period is nil it will be equal to negative
    #   `every`.
    # @param include_boundaries
    #   Add the lower and upper bound of the window to the "_lower_bound" and
    #   "_upper_bound" columns. This will impact performance because it's harder to
    #   parallelize
    # @param closed ["right", "left", "both", "none"]
    #   Define whether the temporal window interval is closed or not.
    # @param label ['left', 'right', 'datapoint']
    #   Define which label to use for the window:
    #
    #   - 'left': lower boundary of the window
    #   - 'right': upper boundary of the window
    #   - 'datapoint': the first value of the index column in the given window.
    #     If you don't need the label to be at one of the boundaries, choose this
    #     option for maximum performance
    # @param group_by
    #   Also group by this column/these columns
    # @param start_by ['window', 'datapoint', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    #   The strategy to determine the start of the first window by.
    #
    #   * 'window': Start by taking the earliest timestamp, truncating it with
    #     `every`, and then adding `offset`.
    #     Note that weekly windows start on Monday.
    #   * 'datapoint': Start from the first encountered data point.
    #   * a day of the week (only takes effect if `every` contains `'w'`):
    #
    #     * 'monday': Start the window on the Monday before the first data point.
    #     * 'tuesday': Start the window on the Tuesday before the first data point.
    #     * ...
    #     * 'sunday': Start the window on the Sunday before the first data point.
    #
    #     The resulting window is then shifted back until the earliest datapoint
    #     is in or in front of it.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => Polars.datetime_range(
    #         DateTime.new(2021, 12, 16),
    #         DateTime.new(2021, 12, 16, 3),
    #         "30m",
    #         time_unit: "us",
    #         eager: true
    #       ),
    #       "n" => 0..6
    #     }
    #   )
    #   # =>
    #   # shape: (7, 2)
    #   # ┌─────────────────────┬─────┐
    #   # │ time                ┆ n   │
    #   # │ ---                 ┆ --- │
    #   # │ datetime[μs]        ┆ i64 │
    #   # ╞═════════════════════╪═════╡
    #   # │ 2021-12-16 00:00:00 ┆ 0   │
    #   # │ 2021-12-16 00:30:00 ┆ 1   │
    #   # │ 2021-12-16 01:00:00 ┆ 2   │
    #   # │ 2021-12-16 01:30:00 ┆ 3   │
    #   # │ 2021-12-16 02:00:00 ┆ 4   │
    #   # │ 2021-12-16 02:30:00 ┆ 5   │
    #   # │ 2021-12-16 03:00:00 ┆ 6   │
    #   # └─────────────────────┴─────┘
    #
    # @example Group by windows of 1 hour starting at 2021-12-16 00:00:00.
    #   df.group_by_dynamic("time", every: "1h", closed: "right").agg(
    #     [
    #       Polars.col("time").min.alias("time_min"),
    #       Polars.col("time").max.alias("time_max")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┐
    #   # │ time                ┆ time_min            ┆ time_max            │
    #   # │ ---                 ┆ ---                 ┆ ---                 │
    #   # │ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-16 00:00:00 │
    #   # │ 2021-12-16 00:00:00 ┆ 2021-12-16 00:30:00 ┆ 2021-12-16 01:00:00 │
    #   # │ 2021-12-16 01:00:00 ┆ 2021-12-16 01:30:00 ┆ 2021-12-16 02:00:00 │
    #   # │ 2021-12-16 02:00:00 ┆ 2021-12-16 02:30:00 ┆ 2021-12-16 03:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┘
    #
    # @example The window boundaries can also be added to the aggregation result.
    #   df.group_by_dynamic(
    #     "time", every: "1h", include_boundaries: true, closed: "right"
    #   ).agg([Polars.col("time").count.alias("time_count")])
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
    #   # │ _lower_boundary     ┆ _upper_boundary     ┆ time                ┆ time_count │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        ┆ u32        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ 2021-12-16 00:00:00 ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 00:00:00 ┆ 2          │
    #   # │ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 2          │
    #   # │ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 2          │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴────────────┘
    #
    # @example When closed="left", should not include right end of interval.
    #   df.group_by_dynamic("time", every: "1h", closed: "left").agg(
    #     [
    #       Polars.col("time").count.alias("time_count"),
    #       Polars.col("time").alias("time_agg_list")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬────────────┬─────────────────────────────────┐
    #   # │ time                ┆ time_count ┆ time_agg_list                   │
    #   # │ ---                 ┆ ---        ┆ ---                             │
    #   # │ datetime[μs]        ┆ u32        ┆ list[datetime[μs]]              │
    #   # ╞═════════════════════╪════════════╪═════════════════════════════════╡
    #   # │ 2021-12-16 00:00:00 ┆ 2          ┆ [2021-12-16 00:00:00, 2021-12-… │
    #   # │ 2021-12-16 01:00:00 ┆ 2          ┆ [2021-12-16 01:00:00, 2021-12-… │
    #   # │ 2021-12-16 02:00:00 ┆ 2          ┆ [2021-12-16 02:00:00, 2021-12-… │
    #   # │ 2021-12-16 03:00:00 ┆ 1          ┆ [2021-12-16 03:00:00]           │
    #   # └─────────────────────┴────────────┴─────────────────────────────────┘
    #
    # @example When closed="both" the time values at the window boundaries belong to 2 groups.
    #   df.group_by_dynamic("time", every: "1h", closed: "both").agg(
    #     [Polars.col("time").count.alias("time_count")]
    #   )
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────────────────────┬────────────┐
    #   # │ time                ┆ time_count │
    #   # │ ---                 ┆ ---        │
    #   # │ datetime[μs]        ┆ u32        │
    #   # ╞═════════════════════╪════════════╡
    #   # │ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ 2021-12-16 00:00:00 ┆ 3          │
    #   # │ 2021-12-16 01:00:00 ┆ 3          │
    #   # │ 2021-12-16 02:00:00 ┆ 3          │
    #   # │ 2021-12-16 03:00:00 ┆ 1          │
    #   # └─────────────────────┴────────────┘
    #
    # @example Dynamic group bys can also be combined with grouping on normal keys.
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => Polars.datetime_range(
    #         DateTime.new(2021, 12, 16),
    #         DateTime.new(2021, 12, 16, 3),
    #         "30m",
    #         time_unit: "us",
    #         eager: true
    #       ),
    #       "groups" => ["a", "a", "a", "b", "b", "a", "a"]
    #     }
    #   )
    #   df.group_by_dynamic(
    #     "time",
    #     every: "1h",
    #     closed: "both",
    #     group_by: "groups",
    #     include_boundaries: true
    #   ).agg([Polars.col("time").count.alias("time_count")])
    #   # =>
    #   # shape: (7, 5)
    #   # ┌────────┬─────────────────────┬─────────────────────┬─────────────────────┬────────────┐
    #   # │ groups ┆ _lower_boundary     ┆ _upper_boundary     ┆ time                ┆ time_count │
    #   # │ ---    ┆ ---                 ┆ ---                 ┆ ---                 ┆ ---        │
    #   # │ str    ┆ datetime[μs]        ┆ datetime[μs]        ┆ datetime[μs]        ┆ u32        │
    #   # ╞════════╪═════════════════════╪═════════════════════╪═════════════════════╪════════════╡
    #   # │ a      ┆ 2021-12-15 23:00:00 ┆ 2021-12-16 00:00:00 ┆ 2021-12-15 23:00:00 ┆ 1          │
    #   # │ a      ┆ 2021-12-16 00:00:00 ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 00:00:00 ┆ 3          │
    #   # │ a      ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 1          │
    #   # │ a      ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 2          │
    #   # │ a      ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 04:00:00 ┆ 2021-12-16 03:00:00 ┆ 1          │
    #   # │ b      ┆ 2021-12-16 01:00:00 ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 01:00:00 ┆ 2          │
    #   # │ b      ┆ 2021-12-16 02:00:00 ┆ 2021-12-16 03:00:00 ┆ 2021-12-16 02:00:00 ┆ 1          │
    #   # └────────┴─────────────────────┴─────────────────────┴─────────────────────┴────────────┘
    #
    # @example Dynamic group by on an index column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "idx" => Polars.arange(0, 6, eager: true),
    #       "A" => ["A", "A", "B", "B", "B", "C"]
    #     }
    #   )
    #   df.group_by_dynamic(
    #     "idx",
    #     every: "2i",
    #     period: "3i",
    #     include_boundaries: true,
    #     closed: "right"
    #   ).agg(Polars.col("A").alias("A_agg_list"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────────────────┬─────────────────┬─────┬─────────────────┐
    #   # │ _lower_boundary ┆ _upper_boundary ┆ idx ┆ A_agg_list      │
    #   # │ ---             ┆ ---             ┆ --- ┆ ---             │
    #   # │ i64             ┆ i64             ┆ i64 ┆ list[str]       │
    #   # ╞═════════════════╪═════════════════╪═════╪═════════════════╡
    #   # │ -2              ┆ 1               ┆ -2  ┆ ["A", "A"]      │
    #   # │ 0               ┆ 3               ┆ 0   ┆ ["A", "B", "B"] │
    #   # │ 2               ┆ 5               ┆ 2   ┆ ["B", "B", "C"] │
    #   # │ 4               ┆ 7               ┆ 4   ┆ ["C"]           │
    #   # └─────────────────┴─────────────────┴─────┴─────────────────┘
    def group_by_dynamic(
      index_column,
      every:,
      period: nil,
      offset: nil,
      include_boundaries: false,
      closed: "left",
      label: "left",
      group_by: nil,
      start_by: "window"
    )
      DynamicGroupBy.new(
        self,
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
    end

    # Upsample a DataFrame at a regular frequency.
    #
    # @param time_column [Object]
    #   time column will be used to determine a date_range.
    #   Note that this column has to be sorted for the output to make sense.
    # @param every [String]
    #   interval will start 'every' duration
    # @param group_by [Object]
    #   First group by these columns and then upsample for every group
    # @param maintain_order [Boolean]
    #   Keep the ordering predictable. This is slower.
    #
    # The `every` and `offset` arguments are created with
    # the following string language:
    #
    # - 1ns   (1 nanosecond)
    # - 1us   (1 microsecond)
    # - 1ms   (1 millisecond)
    # - 1s    (1 second)
    # - 1m    (1 minute)
    # - 1h    (1 hour)
    # - 1d    (1 day)
    # - 1w    (1 week)
    # - 1mo   (1 calendar month)
    # - 1y    (1 calendar year)
    # - 1i    (1 index count)
    #
    # Or combine them:
    # "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @return [DataFrame]
    #
    # @example Upsample a DataFrame by a certain interval.
    #   df = Polars::DataFrame.new(
    #     {
    #       "time" => [
    #         DateTime.new(2021, 2, 1),
    #         DateTime.new(2021, 4, 1),
    #         DateTime.new(2021, 5, 1),
    #         DateTime.new(2021, 6, 1)
    #       ],
    #       "groups" => ["A", "B", "A", "B"],
    #       "values" => [0, 1, 2, 3]
    #     }
    #   ).set_sorted("time")
    #   df.upsample(
    #     time_column: "time", every: "1mo", group_by: "groups", maintain_order: true
    #   ).select(Polars.all.forward_fill)
    #   # =>
    #   # shape: (7, 3)
    #   # ┌─────────────────────┬────────┬────────┐
    #   # │ time                ┆ groups ┆ values │
    #   # │ ---                 ┆ ---    ┆ ---    │
    #   # │ datetime[ns]        ┆ str    ┆ i64    │
    #   # ╞═════════════════════╪════════╪════════╡
    #   # │ 2021-02-01 00:00:00 ┆ A      ┆ 0      │
    #   # │ 2021-03-01 00:00:00 ┆ A      ┆ 0      │
    #   # │ 2021-04-01 00:00:00 ┆ A      ┆ 0      │
    #   # │ 2021-05-01 00:00:00 ┆ A      ┆ 2      │
    #   # │ 2021-04-01 00:00:00 ┆ B      ┆ 1      │
    #   # │ 2021-05-01 00:00:00 ┆ B      ┆ 1      │
    #   # │ 2021-06-01 00:00:00 ┆ B      ┆ 3      │
    #   # └─────────────────────┴────────┴────────┘
    def upsample(
      time_column:,
      every:,
      group_by: nil,
      maintain_order: false
    )
      if group_by.nil?
        group_by = []
      end
      if group_by.is_a?(::String)
        group_by = [group_by]
      end

      every = Utils.parse_as_duration_string(every)

      _from_rbdf(
        _df.upsample(group_by, time_column, every, maintain_order)
      )
    end

    # Perform an asof join.
    #
    # This is similar to a left-join except that we match on nearest key rather than
    # equal keys.
    #
    # Both DataFrames must be sorted by the asof_join key.
    #
    # For each row in the left DataFrame:
    #
    # - A "backward" search selects the last row in the right DataFrame whose 'on' key is less than or equal to the left's key.
    # - A "forward" search selects the first row in the right DataFrame whose 'on' key is greater than or equal to the left's key.
    #
    # The default is "backward".
    #
    # @param other [DataFrame]
    #   DataFrame to join with.
    # @param left_on [String]
    #   Join column of the left DataFrame.
    # @param right_on [String]
    #   Join column of the right DataFrame.
    # @param on [String]
    #   Join column of both DataFrames. If set, `left_on` and `right_on` should be
    #   nil.
    # @param by_left [Object]
    #   join on these columns before doing asof join
    # @param by_right [Object]
    #   join on these columns before doing asof join
    # @param by [Object]
    #   join on these columns before doing asof join
    # @param strategy ["backward", "forward"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param tolerance [Object]
    #   Numeric tolerance. By setting this the join will only be done if the near
    #   keys are within this distance. If an asof join is done on columns of dtype
    #   "Date", "Datetime", "Duration" or "Time" you use the following string
    #   language:
    #
    #    - 1ns   (1 nanosecond)
    #    - 1us   (1 microsecond)
    #    - 1ms   (1 millisecond)
    #    - 1s    (1 second)
    #    - 1m    (1 minute)
    #    - 1h    (1 hour)
    #    - 1d    (1 day)
    #    - 1w    (1 week)
    #    - 1mo   (1 calendar month)
    #    - 1y    (1 calendar year)
    #    - 1i    (1 index count)
    #
    #    Or combine them:
    #    "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
    #
    # @param allow_parallel [Boolean]
    #   Allow the physical plan to optionally evaluate the computation of both
    #   DataFrames up to the join in parallel.
    # @param force_parallel [Boolean]
    #   Force the physical plan to evaluate the computation of both DataFrames up to
    #   the join in parallel.
    # @param coalesce [Boolean]
    #   Coalescing behavior (merging of join columns).
    #     - true: -> Always coalesce join columns.
    #     - false: -> Never coalesce join columns.
    #   Note that joining on any other expressions than `col` will turn off coalescing.
    # @param allow_exact_matches [Boolean]
    #   Whether exact matches are valid join predicates.
    #     - If true, allow matching with the same `on` value (i.e. less-than-or-equal-to / greater-than-or-equal-to).
    #     - If false, don't match the same `on` value (i.e., strictly less-than / strictly greater-than).
    # @param check_sortedness [Boolean]
    #   Check the sortedness of the asof keys. If the keys are not sorted Polars
    #   will error, or in case of 'by' argument raise a warning. This might become
    #   a hard error in the future.
    #
    # @return [DataFrame]
    #
    # @example
    #   gdp = Polars::DataFrame.new(
    #     {
    #       "date" => [
    #         DateTime.new(2016, 1, 1),
    #         DateTime.new(2017, 1, 1),
    #         DateTime.new(2018, 1, 1),
    #         DateTime.new(2019, 1, 1),
    #       ],  # note record date: Jan 1st (sorted!)
    #       "gdp" => [4164, 4411, 4566, 4696]
    #     }
    #   ).set_sorted("date")
    #   population = Polars::DataFrame.new(
    #     {
    #       "date" => [
    #         DateTime.new(2016, 5, 12),
    #         DateTime.new(2017, 5, 12),
    #         DateTime.new(2018, 5, 12),
    #         DateTime.new(2019, 5, 12),
    #       ],  # note record date: May 12th (sorted!)
    #       "population" => [82.19, 82.66, 83.12, 83.52]
    #     }
    #   ).set_sorted("date")
    #   population.join_asof(
    #     gdp, left_on: "date", right_on: "date", strategy: "backward"
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────────────┬────────────┬──────┐
    #   # │ date                ┆ population ┆ gdp  │
    #   # │ ---                 ┆ ---        ┆ ---  │
    #   # │ datetime[ns]        ┆ f64        ┆ i64  │
    #   # ╞═════════════════════╪════════════╪══════╡
    #   # │ 2016-05-12 00:00:00 ┆ 82.19      ┆ 4164 │
    #   # │ 2017-05-12 00:00:00 ┆ 82.66      ┆ 4411 │
    #   # │ 2018-05-12 00:00:00 ┆ 83.12      ┆ 4566 │
    #   # │ 2019-05-12 00:00:00 ┆ 83.52      ┆ 4696 │
    #   # └─────────────────────┴────────────┴──────┘
    def join_asof(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      by_left: nil,
      by_right: nil,
      by: nil,
      strategy: "backward",
      suffix: "_right",
      tolerance: nil,
      allow_parallel: true,
      force_parallel: false,
      coalesce: true,
      allow_exact_matches: true,
      check_sortedness: true
    )
      lazy
        .join_asof(
          other.lazy,
          left_on: left_on,
          right_on: right_on,
          on: on,
          by_left: by_left,
          by_right: by_right,
          by: by,
          strategy: strategy,
          suffix: suffix,
          tolerance: tolerance,
          allow_parallel: allow_parallel,
          force_parallel: force_parallel,
          coalesce: coalesce,
          allow_exact_matches: allow_exact_matches,
          check_sortedness: check_sortedness
        )
        .collect(no_optimization: true)
    end

    # Join in SQL-like fashion.
    #
    # @param other [DataFrame]
    #   DataFrame to join with.
    # @param left_on [Object]
    #   Name(s) of the left join column(s).
    # @param right_on [Object]
    #   Name(s) of the right join column(s).
    # @param on [Object]
    #   Name(s) of the join columns in both DataFrames.
    # @param how ["inner", "left", "full", "semi", "anti", "cross"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    # @param validate ['m:m', 'm:1', '1:m', '1:1']
    #   Checks if join is of specified type.
    #     * *many_to_many* - “m:m”: default, does not result in checks
    #     * *one_to_one* - “1:1”: check if join keys are unique in both left and right datasets
    #     * *one_to_many* - “1:m”: check if join keys are unique in left dataset
    #     * *many_to_one* - “m:1”: check if join keys are unique in right dataset
    # @param nulls_equal [Boolean]
    #   Join on null values. By default null values will never produce matches.
    # @param coalesce [Boolean]
    #   Coalescing behavior (merging of join columns).
    #     - nil: -> join specific.
    #     - true: -> Always coalesce join columns.
    #     - false: -> Never coalesce join columns.
    #   Note that joining on any other expressions than `col` will turn off coalescing.
    # @param maintain_order ['none', 'left', 'right', 'left_right', 'right_left']
    #   Which DataFrame row order to preserve, if any.
    #   Do not rely on any observed ordering without explicitly
    #   setting this parameter, as your code may break in a future release.
    #   Not specifying any ordering can improve performance
    #   Supported for inner, left, right and full joins
    #
    #   * *none*
    #       No specific ordering is desired. The ordering might differ across
    #       Polars versions or even between different runs.
    #   * *left*
    #       Preserves the order of the left DataFrame.
    #   * *right*
    #       Preserves the order of the right DataFrame.
    #   * *left_right*
    #       First preserves the order of the left DataFrame, then the right.
    #   * *right_left*
    #       First preserves the order of the right DataFrame, then the left.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   other_df = Polars::DataFrame.new(
    #     {
    #       "apple" => ["x", "y", "z"],
    #       "ham" => ["a", "b", "d"]
    #     }
    #   )
    #   df.join(other_df, on: "ham")
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str ┆ str   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6.0 ┆ a   ┆ x     │
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "full")
    #   # =>
    #   # shape: (4, 5)
    #   # ┌──────┬──────┬──────┬───────┬───────────┐
    #   # │ foo  ┆ bar  ┆ ham  ┆ apple ┆ ham_right │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---   ┆ ---       │
    #   # │ i64  ┆ f64  ┆ str  ┆ str   ┆ str       │
    #   # ╞══════╪══════╪══════╪═══════╪═══════════╡
    #   # │ 1    ┆ 6.0  ┆ a    ┆ x     ┆ a         │
    #   # │ 2    ┆ 7.0  ┆ b    ┆ y     ┆ b         │
    #   # │ null ┆ null ┆ null ┆ z     ┆ d         │
    #   # │ 3    ┆ 8.0  ┆ c    ┆ null  ┆ null      │
    #   # └──────┴──────┴──────┴───────┴───────────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "left")
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ str ┆ str   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6.0 ┆ a   ┆ x     │
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # │ 3   ┆ 8.0 ┆ c   ┆ null  │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "semi")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # └─────┴─────┴─────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "anti")
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # └─────┴─────┴─────┘
    def join(
      other,
      left_on: nil,
      right_on: nil,
      on: nil,
      how: "inner",
      suffix: "_right",
      validate: "m:m",
      nulls_equal: false,
      coalesce: nil,
      maintain_order: nil
    )
      lazy
        .join(
          other.lazy,
          left_on: left_on,
          right_on: right_on,
          on: on,
          how: how,
          suffix: suffix,
          validate: validate,
          nulls_equal: nulls_equal,
          coalesce: coalesce,
          maintain_order: maintain_order
        )
        .collect(no_optimization: true)
    end

    # Perform a join based on one or multiple (in)equality predicates.
    #
    # This performs an inner join, so only rows where all predicates are true
    # are included in the result, and a row from either DataFrame may be included
    # multiple times in the result.
    #
    # @note
    #   The row order of the input DataFrames is not preserved.
    #
    # @note
    #   This functionality is experimental. It may be
    #   changed at any point without it being considered a breaking change.
    #
    # @param other [DataFrame]
    #   DataFrame to join with.
    # @param predicates [Array]
    #   (In)Equality condition to join the two tables on.
    #   When a column name occurs in both tables, the proper suffix must
    #   be applied in the predicate.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
    #
    # @return [DataFrame]
    #
    # @example Join two dataframes together based on two predicates which get AND-ed together.
    #   east = Polars::DataFrame.new(
    #     {
    #       "id": [100, 101, 102],
    #       "dur": [120, 140, 160],
    #       "rev": [12, 14, 16],
    #       "cores": [2, 8, 4]
    #     }
    #   )
    #   west = Polars::DataFrame.new(
    #     {
    #       "t_id": [404, 498, 676, 742],
    #       "time": [90, 130, 150, 170],
    #       "cost": [9, 13, 15, 16],
    #       "cores": [4, 2, 1, 4]
    #     }
    #   )
    #   east.join_where(
    #     west,
    #     Polars.col("dur") < Polars.col("time"),
    #     Polars.col("rev") < Polars.col("cost")
    #   )
    #   # =>
    #   # shape: (5, 8)
    #   # ┌─────┬─────┬─────┬───────┬──────┬──────┬──────┬─────────────┐
    #   # │ id  ┆ dur ┆ rev ┆ cores ┆ t_id ┆ time ┆ cost ┆ cores_right │
    #   # │ --- ┆ --- ┆ --- ┆ ---   ┆ ---  ┆ ---  ┆ ---  ┆ ---         │
    #   # │ i64 ┆ i64 ┆ i64 ┆ i64   ┆ i64  ┆ i64  ┆ i64  ┆ i64         │
    #   # ╞═════╪═════╪═════╪═══════╪══════╪══════╪══════╪═════════════╡
    #   # │ 100 ┆ 120 ┆ 12  ┆ 2     ┆ 498  ┆ 130  ┆ 13   ┆ 2           │
    #   # │ 100 ┆ 120 ┆ 12  ┆ 2     ┆ 676  ┆ 150  ┆ 15   ┆ 1           │
    #   # │ 100 ┆ 120 ┆ 12  ┆ 2     ┆ 742  ┆ 170  ┆ 16   ┆ 4           │
    #   # │ 101 ┆ 140 ┆ 14  ┆ 8     ┆ 676  ┆ 150  ┆ 15   ┆ 1           │
    #   # │ 101 ┆ 140 ┆ 14  ┆ 8     ┆ 742  ┆ 170  ┆ 16   ┆ 4           │
    #   # └─────┴─────┴─────┴───────┴──────┴──────┴──────┴─────────────┘
    #
    # @example To OR them together, use a single expression and the `|` operator.
    #   east.join_where(
    #     west,
    #     (Polars.col("dur") < Polars.col("time")) | (Polars.col("rev") < Polars.col("cost"))
    #   )
    #   # =>
    #   # shape: (6, 8)
    #   # ┌─────┬─────┬─────┬───────┬──────┬──────┬──────┬─────────────┐
    #   # │ id  ┆ dur ┆ rev ┆ cores ┆ t_id ┆ time ┆ cost ┆ cores_right │
    #   # │ --- ┆ --- ┆ --- ┆ ---   ┆ ---  ┆ ---  ┆ ---  ┆ ---         │
    #   # │ i64 ┆ i64 ┆ i64 ┆ i64   ┆ i64  ┆ i64  ┆ i64  ┆ i64         │
    #   # ╞═════╪═════╪═════╪═══════╪══════╪══════╪══════╪═════════════╡
    #   # │ 100 ┆ 120 ┆ 12  ┆ 2     ┆ 498  ┆ 130  ┆ 13   ┆ 2           │
    #   # │ 100 ┆ 120 ┆ 12  ┆ 2     ┆ 676  ┆ 150  ┆ 15   ┆ 1           │
    #   # │ 100 ┆ 120 ┆ 12  ┆ 2     ┆ 742  ┆ 170  ┆ 16   ┆ 4           │
    #   # │ 101 ┆ 140 ┆ 14  ┆ 8     ┆ 676  ┆ 150  ┆ 15   ┆ 1           │
    #   # │ 101 ┆ 140 ┆ 14  ┆ 8     ┆ 742  ┆ 170  ┆ 16   ┆ 4           │
    #   # │ 102 ┆ 160 ┆ 16  ┆ 4     ┆ 742  ┆ 170  ┆ 16   ┆ 4           │
    #   # └─────┴─────┴─────┴───────┴──────┴──────┴──────┴─────────────┘
    def join_where(
      other,
      *predicates,
      suffix: "_right"
    )
      Utils.require_same_type(self, other)

      lazy
      .join_where(
        other.lazy,
        *predicates,
        suffix: suffix
      )
      .collect(_eager: true)
    end

    # Apply a custom/user-defined function (UDF) over the rows of the DataFrame.
    #
    # The UDF will receive each row as a tuple of values: `udf(row)`.
    #
    # Implementing logic using a Ruby function is almost always _significantly_
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
    # @param return_dtype [Symbol]
    #   Output type of the operation. If none given, Polars tries to infer the type.
    # @param inference_size [Integer]
    #   Only used in the case when the custom function returns rows.
    #   This uses the first `n` rows to determine the output schema
    #
    # @return [Object]
    #
    # @note
    #   The frame-level `apply` cannot track column names (as the UDF is a black-box
    #   that may arbitrarily drop, rearrange, transform, or add new columns); if you
    #   want to apply a UDF such that column names are preserved, you should use the
    #   expression-level `apply` syntax instead.
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [-1, 5, 8]})
    #
    # @example Return a DataFrame by mapping each row to a tuple:
    #   df.map_rows { |t| [t[0] * 2, t[1] * 3] }
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────────┬──────────┐
    #   # │ column_0 ┆ column_1 │
    #   # │ ---      ┆ ---      │
    #   # │ i64      ┆ i64      │
    #   # ╞══════════╪══════════╡
    #   # │ 2        ┆ -3       │
    #   # │ 4        ┆ 15       │
    #   # │ 6        ┆ 24       │
    #   # └──────────┴──────────┘
    #
    # @example Return a Series by mapping each row to a scalar:
    #   df.map_rows { |t| t[0] * 2 + t[1] }
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ map │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 9   │
    #   # │ 14  │
    #   # └─────┘
    def map_rows(return_dtype: nil, inference_size: 256, &function)
      out, is_df = _df.map_rows(function, return_dtype, inference_size)
      if is_df
        _from_rbdf(out)
      else
        _from_rbdf(Utils.wrap_s(out).to_frame._df)
      end
    end

    # Return a new DataFrame grown horizontally by stacking multiple Series to it.
    #
    # @param columns [Object]
    #   Series to stack.
    # @param in_place [Boolean]
    #   Modify in place.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   x = Polars::Series.new("apple", [10, 20, 30])
    #   df.hstack([x])
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ ham ┆ apple │
    #   # │ --- ┆ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ i64 ┆ str ┆ i64   │
    #   # ╞═════╪═════╪═════╪═══════╡
    #   # │ 1   ┆ 6   ┆ a   ┆ 10    │
    #   # │ 2   ┆ 7   ┆ b   ┆ 20    │
    #   # │ 3   ┆ 8   ┆ c   ┆ 30    │
    #   # └─────┴─────┴─────┴───────┘
    def hstack(columns, in_place: false)
      if !columns.is_a?(::Array)
        columns = columns.get_columns
      end
      if in_place
        _df.hstack_mut(columns.map(&:_s))
        self
      else
        _from_rbdf(_df.hstack(columns.map(&:_s)))
      end
    end

    # Grow this DataFrame vertically by stacking a DataFrame to it.
    #
    # @param other [DataFrame]
    #   DataFrame to stack.
    # @param in_place [Boolean]
    #   Modify in place
    #
    # @return [DataFrame]
    #
    # @example
    #   df1 = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2],
    #       "bar" => [6, 7],
    #       "ham" => ["a", "b"]
    #     }
    #   )
    #   df2 = Polars::DataFrame.new(
    #     {
    #       "foo" => [3, 4],
    #       "bar" => [8, 9],
    #       "ham" => ["c", "d"]
    #     }
    #   )
    #   df1.vstack(df2)
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # │ 3   ┆ 8   ┆ c   │
    #   # │ 4   ┆ 9   ┆ d   │
    #   # └─────┴─────┴─────┘
    def vstack(other, in_place: false)
      if in_place
        _df.vstack_mut(other._df)
        self
      else
        _from_rbdf(_df.vstack(other._df))
      end
    end

    # Extend the memory backed by this `DataFrame` with the values from `other`.
    #
    # Different from `vstack` which adds the chunks from `other` to the chunks of this
    # `DataFrame` `extend` appends the data from `other` to the underlying memory
    # locations and thus may cause a reallocation.
    #
    # If this does not cause a reallocation, the resulting data structure will not
    # have any extra chunks and thus will yield faster queries.
    #
    # Prefer `extend` over `vstack` when you want to do a query after a single append.
    # For instance during online operations where you add `n` rows and rerun a query.
    #
    # Prefer `vstack` over `extend` when you want to append many times before doing a
    # query. For instance when you read in multiple files and when to store them in a
    # single `DataFrame`. In the latter case, finish the sequence of `vstack`
    # operations with a `rechunk`.
    #
    # @param other [DataFrame]
    #   DataFrame to vertically add.
    #
    # @return [DataFrame]
    #
    # @example
    #   df1 = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df2 = Polars::DataFrame.new({"foo" => [10, 20, 30], "bar" => [40, 50, 60]})
    #   df1.extend(df2)
    #   # =>
    #   # shape: (6, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 4   │
    #   # │ 2   ┆ 5   │
    #   # │ 3   ┆ 6   │
    #   # │ 10  ┆ 40  │
    #   # │ 20  ┆ 50  │
    #   # │ 30  ┆ 60  │
    #   # └─────┴─────┘
    def extend(other)
      _df.extend(other._df)
      self
    end

    # Remove column from DataFrame and return as new.
    #
    # @param columns [Object]
    #   Column(s) to drop.
    # @param strict [Boolean]
    #   Validate that all column names exist in the current schema,
    #   and throw an exception if any do not.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.drop("ham")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6.0 │
    #   # │ 2   ┆ 7.0 │
    #   # │ 3   ┆ 8.0 │
    #   # └─────┴─────┘
    #
    # @example Drop multiple columns by passing a list of column names.
    #   df.drop(["bar", "ham"])
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example Use positional arguments to drop multiple columns.
    #   df.drop("foo", "ham")
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ bar │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 6.0 │
    #   # │ 7.0 │
    #   # │ 8.0 │
    #   # └─────┘
    def drop(*columns, strict: true)
      lazy.drop(*columns, strict: strict).collect(_eager: true)
    end

    # Drop in place.
    #
    # @param name [Object]
    #   Column to drop.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.drop_in_place("ham")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'ham' [str]
    #   # [
    #   #         "a"
    #   #         "b"
    #   #         "c"
    #   # ]
    def drop_in_place(name)
      Utils.wrap_s(_df.drop_in_place(name))
    end

    # Drop in place if exists.
    #
    # @param name [Object]
    #   Column to drop.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.delete("ham")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'ham' [str]
    #   # [
    #   #         "a"
    #   #         "b"
    #   #         "c"
    #   # ]
    #
    # @example
    #   df.delete("missing")
    #   # => nil
    def delete(name)
      drop_in_place(name) if include?(name)
    end

    # Cast DataFrame column(s) to the specified dtype(s).
    #
    # @param dtypes [Object]
    #   Mapping of column names (or selector) to dtypes, or a single dtype
    #   to which all columns will be cast.
    # @param strict [Boolean]
    #   Throw an error if a cast could not be done (for instance, due to an
    #   overflow).
    #
    # @return [DataFrame]
    #
    # @example Cast specific frame columns to the specified dtypes:
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6.0, 7.0, 8.0],
    #       "ham" => [Date.new(2020, 1, 2), Date.new(2021, 3, 4), Date.new(2022, 5, 6)]
    #     }
    #   )
    #   df.cast({"foo" => Polars::Float32, "bar" => Polars::UInt8})
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬────────────┐
    #   # │ foo ┆ bar ┆ ham        │
    #   # │ --- ┆ --- ┆ ---        │
    #   # │ f32 ┆ u8  ┆ date       │
    #   # ╞═════╪═════╪════════════╡
    #   # │ 1.0 ┆ 6   ┆ 2020-01-02 │
    #   # │ 2.0 ┆ 7   ┆ 2021-03-04 │
    #   # │ 3.0 ┆ 8   ┆ 2022-05-06 │
    #   # └─────┴─────┴────────────┘
    #
    # @example Cast all frame columns matching one dtype (or dtype group) to another dtype:
    #   df.cast({Polars::Date => Polars::Datetime})
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────────────────────┐
    #   # │ foo ┆ bar ┆ ham                 │
    #   # │ --- ┆ --- ┆ ---                 │
    #   # │ i64 ┆ f64 ┆ datetime[μs]        │
    #   # ╞═════╪═════╪═════════════════════╡
    #   # │ 1   ┆ 6.0 ┆ 2020-01-02 00:00:00 │
    #   # │ 2   ┆ 7.0 ┆ 2021-03-04 00:00:00 │
    #   # │ 3   ┆ 8.0 ┆ 2022-05-06 00:00:00 │
    #   # └─────┴─────┴─────────────────────┘
    #
    # @example Cast all frame columns to the specified dtype:
    #   df.cast(Polars::String).to_h(as_series: false)
    #   # => {"foo"=>["1", "2", "3"], "bar"=>["6.0", "7.0", "8.0"], "ham"=>["2020-01-02", "2021-03-04", "2022-05-06"]}
    def cast(dtypes, strict: true)
      lazy.cast(dtypes, strict: strict).collect(_eager: true)
    end

    # Create an empty copy of the current DataFrame.
    #
    # Returns a DataFrame with identical schema but no data.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [nil, 2, 3, 4],
    #       "b" => [0.5, nil, 2.5, 13],
    #       "c" => [true, true, false, nil]
    #     }
    #   )
    #   df.clear
    #   # =>
    #   # shape: (0, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ bool │
    #   # ╞═════╪═════╪══════╡
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   df.clear(2)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ a    ┆ b    ┆ c    │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ f64  ┆ bool │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ null ┆ null ┆ null │
    #   # └──────┴──────┴──────┘
    def clear(n = 0)
      if n == 0
        _from_rbdf(_df.clear)
      elsif n > 0 || len > 0
        self.class.new(
          schema.to_h { |nm, tp| [nm, Series.new(nm, [], dtype: tp).extend_constant(nil, n)] }
        )
      else
        clone
      end
    end

    # clone handled by initialize_copy

    # Get the DataFrame as a Array of Series.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.get_columns
    #   # =>
    #   # [shape: (3,)
    #   # Series: 'foo' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   # ], shape: (3,)
    #   # Series: 'bar' [i64]
    #   # [
    #   #         4
    #   #         5
    #   #         6
    #   # ]]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   df.get_columns
    #   # =>
    #   # [shape: (4,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   #         4
    #   # ], shape: (4,)
    #   # Series: 'b' [f64]
    #   # [
    #   #         0.5
    #   #         4.0
    #   #         10.0
    #   #         13.0
    #   # ], shape: (4,)
    #   # Series: 'c' [bool]
    #   # [
    #   #         true
    #   #         true
    #   #         false
    #   #         true
    #   # ]]
    def get_columns
      _df.get_columns.map { |s| Utils.wrap_s(s) }
    end

    # Get a single column as Series by name.
    #
    # @param name [String]
    #   Name of the column to retrieve.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.get_column("foo")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'foo' [i64]
    #   # [
    #   #         1
    #   #         2
    #   #         3
    #   # ]
    def get_column(name)
      self[name]
    end

    # Fill null values using the specified value or strategy.
    #
    # @param value [Numeric]
    #   Value used to fill null values.
    # @param strategy [nil, "forward", "backward", "min", "max", "mean", "zero", "one"]
    #   Strategy used to fill null values.
    # @param limit [Integer]
    #   Number of consecutive null values to fill when using the 'forward' or
    #   'backward' strategy.
    # @param matches_supertype [Boolean]
    #   Fill all matching supertype of the fill `value`.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, nil, 4],
    #       "b" => [0.5, 4, nil, 13]
    #     }
    #   )
    #   df.fill_null(99)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 99  ┆ 99.0 │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   df.fill_null(strategy: "forward")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   df.fill_null(strategy: "max")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 4   ┆ 13.0 │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    #
    # @example
    #   df.fill_null(strategy: "zero")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬──────┐
    #   # │ a   ┆ b    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ f64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ 0.5  │
    #   # │ 2   ┆ 4.0  │
    #   # │ 0   ┆ 0.0  │
    #   # │ 4   ┆ 13.0 │
    #   # └─────┴──────┘
    def fill_null(value = nil, strategy: nil, limit: nil, matches_supertype: true)
      _from_rbdf(
        lazy
          .fill_null(value, strategy: strategy, limit: limit, matches_supertype: matches_supertype)
          .collect(no_optimization: true)
          ._df
      )
    end

    # Fill floating point NaN values by an Expression evaluation.
    #
    # @param value [Object]
    #   Value to fill NaN with.
    #
    # @return [DataFrame]
    #
    # @note
    #   Note that floating point NaNs (Not a Number) are not missing values!
    #   To replace missing values, use `fill_null`.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1.5, 2, Float::NAN, 4],
    #       "b" => [0.5, 4, Float::NAN, 13]
    #     }
    #   )
    #   df.fill_nan(99)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬──────┐
    #   # │ a    ┆ b    │
    #   # │ ---  ┆ ---  │
    #   # │ f64  ┆ f64  │
    #   # ╞══════╪══════╡
    #   # │ 1.5  ┆ 0.5  │
    #   # │ 2.0  ┆ 4.0  │
    #   # │ 99.0 ┆ 99.0 │
    #   # │ 4.0  ┆ 13.0 │
    #   # └──────┴──────┘
    def fill_nan(value)
      lazy.fill_nan(value).collect(no_optimization: true)
    end

    # Explode `DataFrame` to long format by exploding a column with Lists.
    #
    # @param columns [Object]
    #   Column of LargeList type.
    # @param more_columns [Array]
    #   Additional names of columns to explode, specified as positional arguments.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "letters" => ["a", "a", "b", "c"],
    #       "numbers" => [[1], [2, 3], [4, 5], [6, 7, 8]]
    #     }
    #   )
    #   df.explode("numbers")
    #   # =>
    #   # shape: (8, 2)
    #   # ┌─────────┬─────────┐
    #   # │ letters ┆ numbers │
    #   # │ ---     ┆ ---     │
    #   # │ str     ┆ i64     │
    #   # ╞═════════╪═════════╡
    #   # │ a       ┆ 1       │
    #   # │ a       ┆ 2       │
    #   # │ a       ┆ 3       │
    #   # │ b       ┆ 4       │
    #   # │ b       ┆ 5       │
    #   # │ c       ┆ 6       │
    #   # │ c       ┆ 7       │
    #   # │ c       ┆ 8       │
    #   # └─────────┴─────────┘
    def explode(columns, *more_columns)
      lazy.explode(columns, *more_columns).collect(no_optimization: true)
    end

    # Create a spreadsheet-style pivot table as a DataFrame.
    #
    # @param on [Object]
    #   Columns whose values will be used as the header of the output DataFrame
    # @param index [Object]
    #   One or multiple keys to group by
    # @param values [Object]
    #   Column values to aggregate. Can be multiple columns if the *columns*
    #   arguments contains multiple columns as well
    # @param aggregate_function ["first", "sum", "max", "min", "mean", "median", "last", "count"]
    #   A predefined aggregate function str or an expression.
    # @param maintain_order [Object]
    #   Sort the grouped keys so that the output order is predictable.
    # @param sort_columns [Object]
    #   Sort the transposed columns by name. Default is by order of discovery.
    # @param separator [String]
    #   Used as separator/delimiter in generated column names in case of multiple
    #   `values` columns.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["one", "one", "two", "two", "one", "two"],
    #       "bar" => ["y", "y", "y", "x", "x", "x"],
    #       "baz" => [1, 2, 3, 4, 5, 6]
    #     }
    #   )
    #   df.pivot("bar", index: "foo", values: "baz", aggregate_function: "sum")
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ y   ┆ x   │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ one ┆ 3   ┆ 5   │
    #   # │ two ┆ 3   ┆ 10  │
    #   # └─────┴─────┴─────┘
    def pivot(
      on,
      index: nil,
      values: nil,
      aggregate_function: nil,
      maintain_order: true,
      sort_columns: false,
      separator: "_"
    )
      index = Utils._expand_selectors(self, index)
      on = Utils._expand_selectors(self, on)
      if !values.nil?
        values = Utils._expand_selectors(self, values)
      end

      if aggregate_function.is_a?(::String)
        case aggregate_function
        when "first"
          aggregate_expr = F.element.first._rbexpr
        when "sum"
          aggregate_expr = F.element.sum._rbexpr
        when "max"
          aggregate_expr = F.element.max._rbexpr
        when "min"
          aggregate_expr = F.element.min._rbexpr
        when "mean"
          aggregate_expr = F.element.mean._rbexpr
        when "median"
          aggregate_expr = F.element.median._rbexpr
        when "last"
          aggregate_expr = F.element.last._rbexpr
        when "len"
          aggregate_expr = F.len._rbexpr
        when "count"
          warn "`aggregate_function: \"count\"` input for `pivot` is deprecated. Use `aggregate_function: \"len\"` instead."
          aggregate_expr = F.len._rbexpr
        else
          raise ArgumentError, "Argument aggregate fn: '#{aggregate_fn}' was not expected."
        end
      elsif aggregate_function.nil?
        aggregate_expr = nil
      else
        aggregate_expr = aggregate_function._rbexpr
      end

      _from_rbdf(
        _df.pivot_expr(
          on,
          index,
          values,
          maintain_order,
          sort_columns,
          aggregate_expr,
          separator
        )
      )
    end

    # Unpivot a DataFrame from wide to long format.
    #
    # Optionally leaves identifiers set.
    #
    # This function is useful to massage a DataFrame into a format where one or more
    # columns are identifier variables (index) while all other columns, considered
    # measured variables (on), are "unpivoted" to the row axis leaving just
    # two non-identifier columns, 'variable' and 'value'.
    #
    # @param on [Object]
    #   Column(s) or selector(s) to use as values variables; if `on`
    #   is empty all columns that are not in `index` will be used.
    # @param index [Object]
    #   Column(s) or selector(s) to use as identifier variables.
    # @param variable_name [Object]
    #   Name to give to the `variable` column. Defaults to "variable"
    # @param value_name [Object]
    #   Name to give to the `value` column. Defaults to "value"
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["x", "y", "z"],
    #       "b" => [1, 3, 5],
    #       "c" => [2, 4, 6]
    #     }
    #   )
    #   df.unpivot(Polars.cs.numeric, index: "a")
    #   # =>
    #   # shape: (6, 3)
    #   # ┌─────┬──────────┬───────┐
    #   # │ a   ┆ variable ┆ value │
    #   # │ --- ┆ ---      ┆ ---   │
    #   # │ str ┆ str      ┆ i64   │
    #   # ╞═════╪══════════╪═══════╡
    #   # │ x   ┆ b        ┆ 1     │
    #   # │ y   ┆ b        ┆ 3     │
    #   # │ z   ┆ b        ┆ 5     │
    #   # │ x   ┆ c        ┆ 2     │
    #   # │ y   ┆ c        ┆ 4     │
    #   # │ z   ┆ c        ┆ 6     │
    #   # └─────┴──────────┴───────┘
    def unpivot(on, index: nil, variable_name: nil, value_name: nil)
      on = on.nil? ? [] : Utils._expand_selectors(self, on)
      index = index.nil? ? [] : Utils._expand_selectors(self, index)

      _from_rbdf(_df.unpivot(on, index, value_name, variable_name))
    end
    alias_method :melt, :unpivot

    # Unstack a long table to a wide form without doing an aggregation.
    #
    # This can be much faster than a pivot, because it can skip the grouping phase.
    #
    # @note
    #   This functionality is experimental and may be subject to changes
    #   without it being considered a breaking change.
    #
    # @param step Integer
    #   Number of rows in the unstacked frame.
    # @param how ["vertical", "horizontal"]
    #   Direction of the unstack.
    # @param columns [Object]
    #   Column to include in the operation.
    # @param fill_values [Object]
    #   Fill values that don't fit the new size with this value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "col1" => "A".."I",
    #       "col2" => Polars.arange(0, 9, eager: true)
    #     }
    #   )
    #   # =>
    #   # shape: (9, 2)
    #   # ┌──────┬──────┐
    #   # │ col1 ┆ col2 │
    #   # │ ---  ┆ ---  │
    #   # │ str  ┆ i64  │
    #   # ╞══════╪══════╡
    #   # │ A    ┆ 0    │
    #   # │ B    ┆ 1    │
    #   # │ C    ┆ 2    │
    #   # │ D    ┆ 3    │
    #   # │ E    ┆ 4    │
    #   # │ F    ┆ 5    │
    #   # │ G    ┆ 6    │
    #   # │ H    ┆ 7    │
    #   # │ I    ┆ 8    │
    #   # └──────┴──────┘
    #
    # @example
    #   df.unstack(step: 3, how: "vertical")
    #   # =>
    #   # shape: (3, 6)
    #   # ┌────────┬────────┬────────┬────────┬────────┬────────┐
    #   # │ col1_0 ┆ col1_1 ┆ col1_2 ┆ col2_0 ┆ col2_1 ┆ col2_2 │
    #   # │ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
    #   # │ str    ┆ str    ┆ str    ┆ i64    ┆ i64    ┆ i64    │
    #   # ╞════════╪════════╪════════╪════════╪════════╪════════╡
    #   # │ A      ┆ D      ┆ G      ┆ 0      ┆ 3      ┆ 6      │
    #   # │ B      ┆ E      ┆ H      ┆ 1      ┆ 4      ┆ 7      │
    #   # │ C      ┆ F      ┆ I      ┆ 2      ┆ 5      ┆ 8      │
    #   # └────────┴────────┴────────┴────────┴────────┴────────┘
    #
    # @example
    #   df.unstack(step: 3, how: "horizontal")
    #   # =>
    #   # shape: (3, 6)
    #   # ┌────────┬────────┬────────┬────────┬────────┬────────┐
    #   # │ col1_0 ┆ col1_1 ┆ col1_2 ┆ col2_0 ┆ col2_1 ┆ col2_2 │
    #   # │ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
    #   # │ str    ┆ str    ┆ str    ┆ i64    ┆ i64    ┆ i64    │
    #   # ╞════════╪════════╪════════╪════════╪════════╪════════╡
    #   # │ A      ┆ B      ┆ C      ┆ 0      ┆ 1      ┆ 2      │
    #   # │ D      ┆ E      ┆ F      ┆ 3      ┆ 4      ┆ 5      │
    #   # │ G      ┆ H      ┆ I      ┆ 6      ┆ 7      ┆ 8      │
    #   # └────────┴────────┴────────┴────────┴────────┴────────┘
    def unstack(step:, how: "vertical", columns: nil, fill_values: nil)
      if !columns.nil?
        df = select(columns)
      else
        df = self
      end

      height = df.height
      if how == "vertical"
        n_rows = step
        n_cols = (height / n_rows.to_f).ceil
      else
        n_cols = step
        n_rows = (height / n_cols.to_f).ceil
      end

      n_fill = n_cols * n_rows - height

      if n_fill > 0
        if !fill_values.is_a?(::Array)
          fill_values = [fill_values] * df.width
        end

        df = df.select(
          df.get_columns.zip(fill_values).map do |s, next_fill|
            s.extend_constant(next_fill, n_fill)
          end
        )
      end

      if how == "horizontal"
        df = (
          df.with_columns(
            (Polars.arange(0, n_cols * n_rows, eager: true) % n_cols).alias(
              "__sort_order"
            )
          )
          .sort("__sort_order")
          .drop("__sort_order")
        )
      end

      zfill_val = Math.log10(n_cols).floor + 1
      slices =
        df.get_columns.flat_map do |s|
          n_cols.times.map do |slice_nbr|
            s.slice(slice_nbr * n_rows, n_rows).alias("%s_%0#{zfill_val}d" % [s.name, slice_nbr])
          end
        end

      _from_rbdf(DataFrame.new(slices)._df)
    end

    # Split into multiple DataFrames partitioned by groups.
    #
    # @param by [Object]
    #   Groups to partition by.
    # @param maintain_order [Boolean]
    #   Keep predictable output order. This is slower as it requires an extra sort
    #   operation.
    # @param include_key [Boolean]
    #   Include the columns used to partition the DataFrame in the output.
    # @param as_dict [Boolean]
    #   If true, return the partitions in a hash keyed by the distinct group
    #   values instead of an array.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["A", "A", "B", "B", "C"],
    #       "N" => [1, 2, 2, 4, 2],
    #       "bar" => ["k", "l", "m", "m", "l"]
    #     }
    #   )
    #   df.partition_by("foo", maintain_order: true)
    #   # =>
    #   # [shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ A   ┆ 1   ┆ k   │
    #   # │ A   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘, shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ B   ┆ 2   ┆ m   │
    #   # │ B   ┆ 4   ┆ m   │
    #   # └─────┴─────┴─────┘, shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ C   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘]
    #
    # @example
    #   df.partition_by("foo", maintain_order: true, as_dict: true)
    #   # =>
    #   # {"A"=>shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ A   ┆ 1   ┆ k   │
    #   # │ A   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘, "B"=>shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ B   ┆ 2   ┆ m   │
    #   # │ B   ┆ 4   ┆ m   │
    #   # └─────┴─────┴─────┘, "C"=>shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ N   ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ C   ┆ 2   ┆ l   │
    #   # └─────┴─────┴─────┘}
    def partition_by(by, maintain_order: true, include_key: true, as_dict: false)
      if by.is_a?(::String)
        by = [by]
      elsif !by.is_a?(::Array)
        by = Array(by)
      end

      if as_dict
        out = {}
        if by.length == 1
          _df.partition_by(by, maintain_order, include_key).each do |df|
            df = _from_rbdf(df)
            out[df[by][0, 0]] = df
          end
        else
          _df.partition_by(by, maintain_order, include_key).each do |df|
            df = _from_rbdf(df)
            out[df[by].row(0)] = df
          end
        end
        out
      else
        _df.partition_by(by, maintain_order, include_key).map { |df| _from_rbdf(df) }
      end
    end

    # Shift values by the given period.
    #
    # @param n [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #  Fill the resulting null values with this value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.shift(1)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ null ┆ null ┆ null │
    #   # │ 1    ┆ 6    ┆ a    │
    #   # │ 2    ┆ 7    ┆ b    │
    #   # └──────┴──────┴──────┘
    #
    # @example
    #   df.shift(-1)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────┬──────┬──────┐
    #   # │ foo  ┆ bar  ┆ ham  │
    #   # │ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╡
    #   # │ 2    ┆ 7    ┆ b    │
    #   # │ 3    ┆ 8    ┆ c    │
    #   # │ null ┆ null ┆ null │
    #   # └──────┴──────┴──────┘
    def shift(n, fill_value: nil)
      lazy.shift(n, fill_value: fill_value).collect(_eager: true)
    end

    # Get a mask of all duplicated rows in this DataFrame.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 1],
    #       "b" => ["x", "y", "z", "x"],
    #     }
    #   )
    #   df.is_duplicated
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         false
    #   #         true
    #   # ]
    def is_duplicated
      Utils.wrap_s(_df.is_duplicated)
    end

    # Get a mask of all unique rows in this DataFrame.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 1],
    #       "b" => ["x", "y", "z", "x"]
    #     }
    #   )
    #   df.is_unique
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   #         false
    #   # ]
    def is_unique
      Utils.wrap_s(_df.is_unique)
    end

    # Start a lazy query from this point.
    #
    # @return [LazyFrame]
    def lazy
      wrap_ldf(_df.lazy)
    end

    # Select columns from this DataFrame.
    #
    # @param exprs [Array]
    #   Column(s) to select, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names,
    #   other non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to select, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.select("foo")
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # └─────┘
    #
    # @example
    #   df.select(["foo", "bar"])
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 6   │
    #   # │ 2   ┆ 7   │
    #   # │ 3   ┆ 8   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.col("foo") + 1)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 2   │
    #   # │ 3   │
    #   # │ 4   │
    #   # └─────┘
    #
    # @example
    #   df.select([Polars.col("foo") + 1, Polars.col("bar") + 1])
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 7   │
    #   # │ 3   ┆ 8   │
    #   # │ 4   ┆ 9   │
    #   # └─────┴─────┘
    #
    # @example
    #   df.select(Polars.when(Polars.col("foo") > 2).then(10).otherwise(0))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────┐
    #   # │ literal │
    #   # │ ---     │
    #   # │ i32     │
    #   # ╞═════════╡
    #   # │ 0       │
    #   # │ 0       │
    #   # │ 10      │
    #   # └─────────┘
    def select(*exprs, **named_exprs)
      lazy.select(*exprs, **named_exprs).collect(_eager: true)
    end

    # Select columns from this DataFrame.
    #
    # This will run all expression sequentially instead of in parallel.
    # Use this when the work per expression is cheap.
    #
    # @param exprs [Array]
    #   Column(s) to select, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names,
    #   other non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to select, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    def select_seq(*exprs, **named_exprs)
      lazy
      .select_seq(*exprs, **named_exprs)
      .collect(_eager: true)
    end

    # Add columns to this DataFrame.
    #
    # Added columns will replace existing columns with the same name.
    #
    # @param exprs [Array]
    #   Column(s) to add, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names, other
    #   non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to add, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    #
    # @example Pass an expression to add it as a new column.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   df.with_columns((Polars.col("a") ** 2).alias("a^2"))
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬──────┬───────┬─────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2 │
    #   # │ --- ┆ ---  ┆ ---   ┆ --- │
    #   # │ i64 ┆ f64  ┆ bool  ┆ i64 │
    #   # ╞═════╪══════╪═══════╪═════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1   │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4   │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9   │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16  │
    #   # └─────┴──────┴───────┴─────┘
    #
    # @example Added columns will replace existing columns with the same name.
    #   df.with_columns(Polars.col("a").cast(Polars::Float64))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     │
    #   # │ --- ┆ ---  ┆ ---   │
    #   # │ f64 ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╡
    #   # │ 1.0 ┆ 0.5  ┆ true  │
    #   # │ 2.0 ┆ 4.0  ┆ true  │
    #   # │ 3.0 ┆ 10.0 ┆ false │
    #   # │ 4.0 ┆ 13.0 ┆ true  │
    #   # └─────┴──────┴───────┘
    #
    # @example Multiple columns can be added by passing a list of expressions.
    #   df.with_columns(
    #     [
    #       (Polars.col("a") ** 2).alias("a^2"),
    #       (Polars.col("b") / 2).alias("b/2"),
    #       (Polars.col("c").not_).alias("not c"),
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 6)
    #   # ┌─────┬──────┬───────┬─────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2 ┆ b/2  ┆ not c │
    #   # │ --- ┆ ---  ┆ ---   ┆ --- ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪═════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1   ┆ 0.25 ┆ false │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4   ┆ 2.0  ┆ false │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9   ┆ 5.0  ┆ true  │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16  ┆ 6.5  ┆ false │
    #   # └─────┴──────┴───────┴─────┴──────┴───────┘
    #
    # @example Multiple columns also can be added using positional arguments instead of a list.
    #   df.with_columns(
    #     (Polars.col("a") ** 2).alias("a^2"),
    #     (Polars.col("b") / 2).alias("b/2"),
    #     (Polars.col("c").not_).alias("not c"),
    #   )
    #   # =>
    #   # shape: (4, 6)
    #   # ┌─────┬──────┬───────┬─────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2 ┆ b/2  ┆ not c │
    #   # │ --- ┆ ---  ┆ ---   ┆ --- ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ i64 ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪═════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1   ┆ 0.25 ┆ false │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4   ┆ 2.0  ┆ false │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9   ┆ 5.0  ┆ true  │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16  ┆ 6.5  ┆ false │
    #   # └─────┴──────┴───────┴─────┴──────┴───────┘
    #
    # @example Use keyword arguments to easily name your expression inputs.
    #   df.with_columns(
    #     ab: Polars.col("a") * Polars.col("b"),
    #     not_c: Polars.col("c").not_
    #   )
    #   # =>
    #   # shape: (4, 5)
    #   # ┌─────┬──────┬───────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ ab   ┆ not_c │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 0.5  ┆ false │
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 8.0  ┆ false │
    #   # │ 3   ┆ 10.0 ┆ false ┆ 30.0 ┆ true  │
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 52.0 ┆ false │
    #   # └─────┴──────┴───────┴──────┴───────┘
    def with_columns(*exprs, **named_exprs)
      lazy.with_columns(*exprs, **named_exprs).collect(_eager: true)
    end

    # Add columns to this DataFrame.
    #
    # Added columns will replace existing columns with the same name.
    #
    # This will run all expression sequentially instead of in parallel.
    # Use this when the work per expression is cheap.
    #
    # @param exprs [Array]
    #   Column(s) to add, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names, other
    #   non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional columns to add, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [DataFrame]
    def with_columns_seq(
      *exprs,
      **named_exprs
    )
      lazy
      .with_columns_seq(*exprs, **named_exprs)
      .collect(_eager: true)
    end

    # Get number of chunks used by the ChunkedArrays of this DataFrame.
    #
    # @param strategy ["first", "all"]
    #   Return the number of chunks of the 'first' column,
    #   or 'all' columns in this DataFrame.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   df.n_chunks
    #   # => 1
    #   df.n_chunks(strategy: "all")
    #   # => [1, 1, 1]
    def n_chunks(strategy: "first")
      if strategy == "first"
        _df.n_chunks
      elsif strategy == "all"
        get_columns.map(&:n_chunks)
      else
        raise ArgumentError, "Strategy: '{strategy}' not understood. Choose one of {{'first',  'all'}}"
      end
    end

    # Aggregate the columns of this DataFrame to their maximum value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.max
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def max
      lazy.max.collect(_eager: true)
    end

    # Get the maximum value horizontally across columns.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [4.0, 5.0, 6.0]
    #     }
    #   )
    #   df.max_horizontal
    #   # =>
    #   # shape: (3,)
    #   # Series: 'max' [f64]
    #   # [
    #   #         4.0
    #   #         5.0
    #   #         6.0
    #   # ]
    def max_horizontal
      select(max: F.max_horizontal(F.all)).to_series
    end

    # Aggregate the columns of this DataFrame to their minimum value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.min
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # └─────┴─────┴─────┘
    def min
      lazy.min.collect(_eager: true)
    end

    # Get the minimum value horizontally across columns.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [4.0, 5.0, 6.0]
    #     }
    #   )
    #   df.min_horizontal
    #   # =>
    #   # shape: (3,)
    #   # Series: 'min' [f64]
    #   # [
    #   #         1.0
    #   #         2.0
    #   #         3.0
    #   # ]
    def min_horizontal
      select(min: F.min_horizontal(F.all)).to_series
    end

    # Aggregate the columns of this DataFrame to their sum value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"],
    #     }
    #   )
    #   df.sum
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ i64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 6   ┆ 21  ┆ null │
    #   # └─────┴─────┴──────┘
    def sum
      lazy.sum.collect(_eager: true)
    end

    # Sum all values horizontally across columns.
    #
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #   If set to `false`, any null value in the input will lead to a null output.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [4.0, 5.0, 6.0]
    #     }
    #   )
    #   df.sum_horizontal
    #   # =>
    #   # shape: (3,)
    #   # Series: 'sum' [f64]
    #   # [
    #   #         5.0
    #   #         7.0
    #   #         9.0
    #   # ]
    def sum_horizontal(ignore_nulls: true)
      select(
        sum: F.sum_horizontal(F.all, ignore_nulls: ignore_nulls)
      ).to_series
    end

    # Aggregate the columns of this DataFrame to their mean value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.mean
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 2.0 ┆ 7.0 ┆ null │
    #   # └─────┴─────┴──────┘
    def mean
      lazy.mean.collect(_eager: true)
    end

    # Take the mean of all values horizontally across columns.
    #
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #   If set to `false`, any null value in the input will lead to a null output.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [4.0, 5.0, 6.0]
    #     }
    #   )
    #   df.mean_horizontal
    #   # =>
    #   # shape: (3,)
    #   # Series: 'mean' [f64]
    #   # [
    #   #         2.5
    #   #         3.5
    #   #         4.5
    #   # ]
    def mean_horizontal(ignore_nulls: true)
      select(
        mean: F.mean_horizontal(F.all, ignore_nulls: ignore_nulls)
      ).to_series
    end

    # Aggregate the columns of this DataFrame to their standard deviation value.
    #
    # @param ddof [Integer]
    #   Degrees of freedom
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.std
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1.0 ┆ 1.0 ┆ null │
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   df.std(ddof: 0)
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────────┬──────────┬──────┐
    #   # │ foo      ┆ bar      ┆ ham  │
    #   # │ ---      ┆ ---      ┆ ---  │
    #   # │ f64      ┆ f64      ┆ str  │
    #   # ╞══════════╪══════════╪══════╡
    #   # │ 0.816497 ┆ 0.816497 ┆ null │
    #   # └──────────┴──────────┴──────┘
    def std(ddof: 1)
      lazy.std(ddof: ddof).collect(_eager: true)
    end

    # Aggregate the columns of this DataFrame to their variance value.
    #
    # @param ddof [Integer]
    #   Degrees of freedom
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.var
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 1.0 ┆ 1.0 ┆ null │
    #   # └─────┴─────┴──────┘
    #
    # @example
    #   df.var(ddof: 0)
    #   # =>
    #   # shape: (1, 3)
    #   # ┌──────────┬──────────┬──────┐
    #   # │ foo      ┆ bar      ┆ ham  │
    #   # │ ---      ┆ ---      ┆ ---  │
    #   # │ f64      ┆ f64      ┆ str  │
    #   # ╞══════════╪══════════╪══════╡
    #   # │ 0.666667 ┆ 0.666667 ┆ null │
    #   # └──────────┴──────────┴──────┘
    def var(ddof: 1)
      lazy.var(ddof: ddof).collect(_eager: true)
    end

    # Aggregate the columns of this DataFrame to their median value.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.median
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 2.0 ┆ 7.0 ┆ null │
    #   # └─────┴─────┴──────┘
    def median
      lazy.median.collect(_eager: true)
    end

    # Aggregate the columns of this DataFrame to their product values.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => [0.5, 4, 10],
    #       "c" => [true, true, false]
    #     }
    #   )
    #   df.product
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬──────┬─────┐
    #   # │ a   ┆ b    ┆ c   │
    #   # │ --- ┆ ---  ┆ --- │
    #   # │ i64 ┆ f64  ┆ i64 │
    #   # ╞═════╪══════╪═════╡
    #   # │ 6   ┆ 20.0 ┆ 0   │
    #   # └─────┴──────┴─────┘
    def product
      select(Polars.all.product)
    end

    # Aggregate the columns of this DataFrame to their quantile value.
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
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.quantile(0.5, interpolation: "nearest")
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ foo ┆ bar ┆ ham  │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ f64 ┆ f64 ┆ str  │
    #   # ╞═════╪═════╪══════╡
    #   # │ 2.0 ┆ 7.0 ┆ null │
    #   # └─────┴─────┴──────┘
    def quantile(quantile, interpolation: "nearest")
      lazy.quantile(quantile, interpolation: interpolation).collect(_eager: true)
    end

    # Get one hot encoded dummy variables.
    #
    # @param columns [Array]
    #   A subset of columns to convert to dummy variables. `nil` means
    #   "all columns".
    # @param separator [String]
    #   Separator/delimiter used when generating column names.
    # @param drop_first [Boolean]
    #   Remove the first category from the variables being encoded.
    # @param drop_nulls [Boolean]
    #   If there are `nil` values in the series, a `null` column is not generated
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2],
    #       "bar" => [3, 4],
    #       "ham" => ["a", "b"]
    #     }
    #   )
    #   df.to_dummies
    #   # =>
    #   # shape: (2, 6)
    #   # ┌───────┬───────┬───────┬───────┬───────┬───────┐
    #   # │ foo_1 ┆ foo_2 ┆ bar_3 ┆ bar_4 ┆ ham_a ┆ ham_b │
    #   # │ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---   │
    #   # │ u8    ┆ u8    ┆ u8    ┆ u8    ┆ u8    ┆ u8    │
    #   # ╞═══════╪═══════╪═══════╪═══════╪═══════╪═══════╡
    #   # │ 1     ┆ 0     ┆ 1     ┆ 0     ┆ 1     ┆ 0     │
    #   # │ 0     ┆ 1     ┆ 0     ┆ 1     ┆ 0     ┆ 1     │
    #   # └───────┴───────┴───────┴───────┴───────┴───────┘
    def to_dummies(columns: nil, separator: "_", drop_first: false, drop_nulls: false)
      if columns.is_a?(::String)
        columns = [columns]
      end
      _from_rbdf(_df.to_dummies(columns, separator, drop_first, drop_nulls))
    end

    # Drop duplicate rows from this DataFrame.
    #
    # @param maintain_order [Boolean]
    #   Keep the same order as the original DataFrame. This requires more work to
    #   compute.
    # @param subset [Object]
    #   Subset to use to compare rows.
    # @param keep ["first", "last"]
    #   Which of the duplicate rows to keep (in conjunction with `subset`).
    #
    # @return [DataFrame]
    #
    # @note
    #   Note that this fails if there is a column of type `List` in the DataFrame or
    #   subset.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 1, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 1.0, 2.0, 3.0, 3.0],
    #       "c" => [true, true, true, false, true, true]
    #     }
    #   )
    #   df.unique
    #   # =>
    #   # shape: (5, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ a   ┆ b   ┆ c     │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ i64 ┆ f64 ┆ bool  │
    #   # ╞═════╪═════╪═══════╡
    #   # │ 1   ┆ 0.5 ┆ true  │
    #   # │ 2   ┆ 1.0 ┆ true  │
    #   # │ 3   ┆ 2.0 ┆ false │
    #   # │ 4   ┆ 3.0 ┆ true  │
    #   # │ 5   ┆ 3.0 ┆ true  │
    #   # └─────┴─────┴───────┘
    def unique(maintain_order: true, subset: nil, keep: "first")
      self._from_rbdf(
        lazy
          .unique(maintain_order: maintain_order, subset: subset, keep: keep)
          .collect(no_optimization: true)
          ._df
      )
    end

    # Return the number of unique rows, or the number of unique row-subsets.
    #
    # @param subset [Object]
    #   One or more columns/expressions that define what to count;
    #   omit to return the count of unique rows.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 1, 2, 3, 4, 5],
    #       "b" => [0.5, 0.5, 1.0, 2.0, 3.0, 3.0],
    #       "c" => [true, true, true, false, true, true]
    #     }
    #   )
    #   df.n_unique
    #   # => 5
    #
    # @example Simple columns subset
    #   df.n_unique(subset: ["b", "c"])
    #   # => 4
    #
    # @example Expression subset
    #   df.n_unique(
    #     subset: [
    #       (Polars.col("a").floordiv(2)),
    #       (Polars.col("c") | (Polars.col("b") >= 2))
    #     ]
    #   )
    #   # => 3
    def n_unique(subset: nil)
      if subset.is_a?(StringIO)
        subset = [Polars.col(subset)]
      elsif subset.is_a?(Expr)
        subset = [subset]
      end

      if subset.is_a?(::Array) && subset.length == 1
        expr = Utils.wrap_expr(Utils.parse_into_expression(subset[0], str_as_lit: false))
      else
        struct_fields = subset.nil? ? Polars.all : subset
        expr = Polars.struct(struct_fields)
      end

      df = lazy.select(expr.n_unique).collect
      df.is_empty ? 0 : df.row(0)[0]
    end

    # Rechunk the data in this DataFrame to a contiguous allocation.

    # This will make sure all subsequent operations have optimal and predictable
    # performance.
    #
    # @return [DataFrame]
    def rechunk
      _from_rbdf(_df.rechunk)
    end

    # Create a new DataFrame that shows the null counts per column.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 3],
    #       "bar" => [6, 7, nil],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.null_count
    #   # =>
    #   # shape: (1, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ u32 ┆ u32 ┆ u32 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 1   ┆ 0   │
    #   # └─────┴─────┴─────┘
    def null_count
      _from_rbdf(_df.null_count)
    end

    # Sample from this DataFrame.
    #
    # @param n [Integer]
    #   Number of items to return. Cannot be used with `fraction`. Defaults to 1 if
    #   `fraction` is nil.
    # @param fraction [Float]
    #   Fraction of items to return. Cannot be used with `n`.
    # @param with_replacement [Boolean]
    #   Allow values to be sampled more than once.
    # @param shuffle [Boolean]
    #   Shuffle the order of sampled data points.
    # @param seed [Integer]
    #   Seed for the random number generator. If set to nil (default), a random
    #   seed is used.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.sample(n: 2, seed: 0)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    def sample(
      n: nil,
      fraction: nil,
      with_replacement: false,
      shuffle: false,
      seed: nil
    )
      if !n.nil? && !fraction.nil?
        raise ArgumentError, "cannot specify both `n` and `fraction`"
      end

      if n.nil? && !fraction.nil?
        fraction = Series.new("fraction", [fraction]) unless fraction.is_a?(Series)

        return _from_rbdf(
          _df.sample_frac(fraction._s, with_replacement, shuffle, seed)
        )
      end

      if n.nil?
        n = 1
      end

      n = Series.new("", [n]) unless n.is_a?(Series)

      _from_rbdf(_df.sample_n(n._s, with_replacement, shuffle, seed))
    end

    # Apply a horizontal reduction on a DataFrame.
    #
    # This can be used to effectively determine aggregations on a row level, and can
    # be applied to any DataType that can be supercasted (casted to a similar parent
    # type).
    #
    # An example of the supercast rules when applying an arithmetic operation on two
    # DataTypes are for instance:
    #
    # i8 + str = str
    # f32 + i64 = f32
    # f32 + f64 = f64
    #
    # @return [Series]
    #
    # @example A horizontal sum operation:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [2, 1, 3],
    #       "b" => [1, 2, 3],
    #       "c" => [1.0, 2.0, 3.0]
    #     }
    #   )
    #   df.fold { |s1, s2| s1 + s2 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         4.0
    #   #         5.0
    #   #         9.0
    #   # ]
    #
    # @example A horizontal minimum operation:
    #   df = Polars::DataFrame.new({"a" => [2, 1, 3], "b" => [1, 2, 3], "c" => [1.0, 2.0, 3.0]})
    #   df.fold { |s1, s2| s1.zip_with(s1 < s2, s2) }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [f64]
    #   # [
    #   #         1.0
    #   #         1.0
    #   #         3.0
    #   # ]
    #
    # @example A horizontal string concatenation:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => ["foo", "bar", nil],
    #       "b" => [1, 2, 3],
    #       "c" => [1.0, 2.0, 3.0]
    #     }
    #   )
    #   df.fold { |s1, s2| s1 + s2 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [str]
    #   # [
    #   #         "foo11.0"
    #   #         "bar22.0"
    #   #         null
    #   # ]
    #
    # @example A horizontal boolean or, similar to a row-wise .any:
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [false, false, true],
    #       "b" => [false, true, false]
    #     }
    #   )
    #   df.fold { |s1, s2| s1 | s2 }
    #   # =>
    #   # shape: (3,)
    #   # Series: 'a' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   # ]
    def fold
      acc = to_series(0)

      1.upto(width - 1) do |i|
        acc = yield(acc, to_series(i))
      end
      acc
    end

    # Get a row as tuple, either by index or by predicate.
    #
    # @param index [Object]
    #   Row index.
    # @param by_predicate [Object]
    #   Select the row according to a given expression/predicate.
    # @param named [Boolean]
    #   Return a hash instead of an array. The hash is a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    #
    # @return [Object]
    #
    # @note
    #   The `index` and `by_predicate` params are mutually exclusive. Additionally,
    #   to ensure clarity, the `by_predicate` parameter must be supplied by keyword.
    #
    #   When using `by_predicate` it is an error condition if anything other than
    #   one row is returned; more than one row raises `TooManyRowsReturned`, and
    #   zero rows will raise `NoRowsReturned` (both inherit from `RowsException`).
    #
    # @example Return the row at the given index
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, 7, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.row(2)
    #   # => [3, 8, "c"]
    #
    # @example Get a hash instead with a mapping of column names to row values
    #   df.row(2, named: true)
    #   # => {"foo"=>3, "bar"=>8, "ham"=>"c"}
    #
    # @example Return the row that matches the given predicate
    #   df.row(by_predicate: Polars.col("ham") == "b")
    #   # => [2, 7, "b"]
    def row(index = nil, by_predicate: nil, named: false)
      if !index.nil? && !by_predicate.nil?
        raise ArgumentError, "Cannot set both 'index' and 'by_predicate'; mutually exclusive"
      elsif index.is_a?(Expr)
        raise TypeError, "Expressions should be passed to the 'by_predicate' param"
      end

      if !index.nil?
        row = _df.row_tuple(index)
        if named
          columns.zip(row).to_h
        else
          row
        end
      elsif !by_predicate.nil?
        if !by_predicate.is_a?(Expr)
          raise TypeError, "Expected by_predicate to be an expression; found #{by_predicate.class.name}"
        end
        rows = filter(by_predicate).rows
        n_rows = rows.length
        if n_rows > 1
          raise TooManyRowsReturned, "Predicate #{by_predicate} returned #{n_rows} rows"
        elsif n_rows == 0
          raise NoRowsReturned, "Predicate #{by_predicate} returned no rows"
        end
        row = rows[0]
        if named
          columns.zip(row).to_h
        else
          row
        end
      else
        raise ArgumentError, "One of 'index' or 'by_predicate' must be set"
      end
    end

    # Convert columnar data to rows as Ruby arrays.
    #
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    #
    # @return [Array]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.rows
    #   # => [[1, 2], [3, 4], [5, 6]]
    # @example
    #   df.rows(named: true)
    #   # => [{"a"=>1, "b"=>2}, {"a"=>3, "b"=>4}, {"a"=>5, "b"=>6}]
    def rows(named: false)
      if named
        columns = self.columns
        _df.row_tuples.map do |v|
          columns.zip(v).to_h
        end
      else
        _df.row_tuples
      end
    end

    # Convert columnar data to rows as Ruby arrays in a hash keyed by some column.
    #
    # This method is like `rows`, but instead of returning rows in a flat list, rows
    # are grouped by the values in the `key` column(s) and returned as a hash.
    #
    # Note that this method should not be used in place of native operations, due to
    # the high cost of materializing all frame data out into a hash; it should
    # be used only when you need to move the values out into a Ruby data structure
    # or other object that cannot operate directly with Polars/Arrow.
    #
    # @param key [Object]
    #   The column(s) to use as the key for the returned hash. If multiple
    #   columns are specified, the key will be a tuple of those values, otherwise
    #   it will be a string.
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    # @param include_key [Boolean]
    #   Include key values inline with the associated data (by default the key
    #   values are omitted as a memory/performance optimisation, as they can be
    #   reoconstructed from the key).
    # @param unique [Boolean]
    #   Indicate that the key is unique; this will result in a 1:1 mapping from
    #   key to a single associated row. Note that if the key is *not* actually
    #   unique the last row with the given key will be returned.
    #
    # @return [Hash]
    #
    # @example Group rows by the given key column(s):
    #   df = Polars::DataFrame.new(
    #     {
    #       "w" => ["a", "b", "b", "a"],
    #       "x" => ["q", "q", "q", "k"],
    #       "y" => [1.0, 2.5, 3.0, 4.5],
    #       "z" => [9, 8, 7, 6]
    #     }
    #   )
    #   df.rows_by_key(["w"])
    #   # => {"a"=>[["q", 1.0, 9], ["k", 4.5, 6]], "b"=>[["q", 2.5, 8], ["q", 3.0, 7]]}
    #
    # @example Return the same row groupings as hashes:
    #   df.rows_by_key(["w"], named: true)
    #   # => {"a"=>[{"x"=>"q", "y"=>1.0, "z"=>9}, {"x"=>"k", "y"=>4.5, "z"=>6}], "b"=>[{"x"=>"q", "y"=>2.5, "z"=>8}, {"x"=>"q", "y"=>3.0, "z"=>7}]}
    #
    # @example Return row groupings, assuming keys are unique:
    #   df.rows_by_key(["z"], unique: true)
    #   # => {9=>["a", "q", 1.0], 8=>["b", "q", 2.5], 7=>["b", "q", 3.0], 6=>["a", "k", 4.5]}
    #
    # @example Return row groupings as hashes, assuming keys are unique:
    #   df.rows_by_key(["z"], named: true, unique: true)
    #   # => {9=>{"w"=>"a", "x"=>"q", "y"=>1.0}, 8=>{"w"=>"b", "x"=>"q", "y"=>2.5}, 7=>{"w"=>"b", "x"=>"q", "y"=>3.0}, 6=>{"w"=>"a", "x"=>"k", "y"=>4.5}}
    #
    # @example Return hash rows grouped by a compound key, including key values:
    #   df.rows_by_key(["w", "x"], named: true, include_key: true)
    #   # => {["a", "q"]=>[{"w"=>"a", "x"=>"q", "y"=>1.0, "z"=>9}], ["b", "q"]=>[{"w"=>"b", "x"=>"q", "y"=>2.5, "z"=>8}, {"w"=>"b", "x"=>"q", "y"=>3.0, "z"=>7}], ["a", "k"]=>[{"w"=>"a", "x"=>"k", "y"=>4.5, "z"=>6}]}
    def rows_by_key(key, named: false, include_key: false, unique: false)
      key = Utils._expand_selectors(self, key)

      keys = key.size == 1 ? get_column(key[0]) : select(key).iter_rows

      if include_key
        values = self
      else
        data_cols = schema.keys - key
        values = select(data_cols)
      end

      zipped = keys.each.zip(values.iter_rows(named: named))

      # if unique, we expect to write just one entry per key; otherwise, we're
      # returning a list of rows for each key, so append into a hash of arrays.
      if unique
        zipped.to_h
      else
        zipped.each_with_object({}) { |(key, data), h| (h[key] ||= []) << data }
      end
    end

    # Returns an iterator over the DataFrame of rows of Ruby-native values.
    #
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    # @param buffer_size [Integer]
    #   Determines the number of rows that are buffered internally while iterating
    #   over the data; you should only modify this in very specific cases where the
    #   default value is determined not to be a good fit to your access pattern, as
    #   the speedup from using the buffer is significant (~2-4x). Setting this
    #   value to zero disables row buffering.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.iter_rows.map { |row| row[0] }
    #   # => [1, 3, 5]
    #
    # @example
    #   df.iter_rows(named: true).map { |row| row["b"] }
    #   # => [2, 4, 6]
    def iter_rows(named: false, buffer_size: 500, &block)
      return to_enum(:iter_rows, named: named, buffer_size: buffer_size) unless block_given?

      # load into the local namespace for a modest performance boost in the hot loops
      columns = self.columns

      # note: buffering rows results in a 2-4x speedup over individual calls
      # to ".row(i)", so it should only be disabled in extremely specific cases.
      if buffer_size
        offset = 0
        while offset < height
          zerocopy_slice = slice(offset, buffer_size)
          rows_chunk = zerocopy_slice.rows(named: false)
          if named
            rows_chunk.each do |row|
              yield columns.zip(row).to_h
            end
          else
            rows_chunk.each(&block)
          end
          offset += buffer_size
        end
      elsif named
        height.times do |i|
          yield columns.zip(row(i)).to_h
        end
      else
        height.times do |i|
          yield row(i)
        end
      end
    end

    # Returns an iterator over the DataFrame of rows of Ruby-native values.
    #
    # @param named [Boolean]
    #   Return hashes instead of arrays. The hashes are a mapping of
    #   column name to row value. This is more expensive than returning an
    #   array, but allows for accessing values by column name.
    # @param buffer_size [Integer]
    #   Determines the number of rows that are buffered internally while iterating
    #   over the data; you should only modify this in very specific cases where the
    #   default value is determined not to be a good fit to your access pattern, as
    #   the speedup from using the buffer is significant (~2-4x). Setting this
    #   value to zero disables row buffering.
    #
    # @return [Object]
    def each_row(named: true, buffer_size: 500, &block)
      iter_rows(named: named, buffer_size: buffer_size, &block)
    end

    # Returns an iterator over the columns of this DataFrame.
    #
    # @return [Object]
    #
    # @note
    #   Consider whether you can use `all` instead.
    #   If you can, it will be more efficient.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.iter_columns.map { |s| s.name }
    #   # => ["a", "b"]
    #
    # @example If you're using this to modify a dataframe's columns, e.g.
    #   # Do NOT do this
    #   Polars::DataFrame.new(df.iter_columns.map { |column| column * 2 })
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 4   │
    #   # │ 6   ┆ 8   │
    #   # │ 10  ┆ 12  │
    #   # └─────┴─────┘
    #
    # @example then consider whether you can use `all` instead:
    #   df.select(Polars.all * 2)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 2   ┆ 4   │
    #   # │ 6   ┆ 8   │
    #   # │ 10  ┆ 12  │
    #   # └─────┴─────┘
    def iter_columns
      return to_enum(:iter_columns) unless block_given?

      _df.get_columns.each do |s|
        yield Utils.wrap_s(s)
      end
    end

    # Returns a non-copying iterator of slices over the underlying DataFrame.
    #
    # @param n_rows [Integer]
    #   Determines the number of rows contained in each DataFrame slice.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => 0...17_500,
    #       "b" => Date.new(2023, 1, 1),
    #       "c" => "klmnoopqrstuvwxyz"
    #     },
    #     schema_overrides: {"a" => Polars::Int32}
    #   )
    #   df.iter_slices.map.with_index do |frame, idx|
    #     "#{frame.class.name}:[#{idx}]:#{frame.length}"
    #   end
    #   # => ["Polars::DataFrame:[0]:10000", "Polars::DataFrame:[1]:7500"]
    def iter_slices(n_rows: 10_000)
      return to_enum(:iter_slices, n_rows: n_rows) unless block_given?

      offset = 0
      while offset < height
        yield slice(offset, n_rows)
        offset += n_rows
      end
    end

    # Shrink DataFrame memory usage.
    #
    # Shrinks to fit the exact capacity needed to hold the data.
    #
    # @return [DataFrame]
    def shrink_to_fit(in_place: false)
      if in_place
        _df.shrink_to_fit
        self
      else
        df = clone
        df._df.shrink_to_fit
        df
      end
    end

    # Take every nth row in the DataFrame and return as a new DataFrame.
    #
    # @return [DataFrame]
    #
    # @example
    #   s = Polars::DataFrame.new({"a" => [1, 2, 3, 4], "b" => [5, 6, 7, 8]})
    #   s.gather_every(2)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 5   │
    #   # │ 3   ┆ 7   │
    #   # └─────┴─────┘
    def gather_every(n, offset = 0)
      select(F.col("*").gather_every(n, offset))
    end

    # Hash and combine the rows in this DataFrame.
    #
    # The hash value is of type `:u64`.
    #
    # @param seed [Integer]
    #   Random seed parameter. Defaults to 0.
    # @param seed_1 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    # @param seed_2 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    # @param seed_3 [Integer]
    #   Random seed parameter. Defaults to `seed` if not set.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 3, 4],
    #       "ham" => ["a", "b", nil, "d"]
    #     }
    #   )
    #   df.hash_rows(seed: 42)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u64]
    #   # [
    #   #         4238614331852490969
    #   #         17976148875586754089
    #   #         4702262519505526977
    #   #         18144177983981041107
    #   # ]
    def hash_rows(seed: 0, seed_1: nil, seed_2: nil, seed_3: nil)
      k0 = seed
      k1 = seed_1.nil? ? seed : seed_1
      k2 = seed_2.nil? ? seed : seed_2
      k3 = seed_3.nil? ? seed : seed_3
      Utils.wrap_s(_df.hash_rows(k0, k1, k2, k3))
    end

    # Interpolate intermediate values. The interpolation method is linear.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, nil, 9, 10],
    #       "bar" => [6, 7, 9, nil],
    #       "baz" => [1, nil, nil, 9]
    #     }
    #   )
    #   df.interpolate
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬──────┬──────────┐
    #   # │ foo  ┆ bar  ┆ baz      │
    #   # │ ---  ┆ ---  ┆ ---      │
    #   # │ f64  ┆ f64  ┆ f64      │
    #   # ╞══════╪══════╪══════════╡
    #   # │ 1.0  ┆ 6.0  ┆ 1.0      │
    #   # │ 5.0  ┆ 7.0  ┆ 3.666667 │
    #   # │ 9.0  ┆ 9.0  ┆ 6.333333 │
    #   # │ 10.0 ┆ null ┆ 9.0      │
    #   # └──────┴──────┴──────────┘
    def interpolate
      select(F.col("*").interpolate)
    end

    # Check if the dataframe is empty.
    #
    # @return [Boolean]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   df.is_empty
    #   # => false
    #   df.filter(Polars.col("foo") > 99).is_empty
    #   # => true
    def is_empty
      height == 0
    end
    alias_method :empty?, :is_empty

    # Convert a `DataFrame` to a `Series` of type `Struct`.
    #
    # @param name [String]
    #   Name for the struct Series
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4, 5],
    #       "b" => ["one", "two", "three", "four", "five"]
    #     }
    #   )
    #   df.to_struct("nums")
    #   # =>
    #   # shape: (5,)
    #   # Series: 'nums' [struct[2]]
    #   # [
    #   #         {1,"one"}
    #   #         {2,"two"}
    #   #         {3,"three"}
    #   #         {4,"four"}
    #   #         {5,"five"}
    #   # ]
    def to_struct(name)
      Utils.wrap_s(_df.to_struct(name))
    end

    # Decompose a struct into its fields.
    #
    # The fields will be inserted into the `DataFrame` on the location of the
    # `struct` type.
    #
    # @param columns [Object]
    #   Name of the struct column(s) that should be unnested.
    # @param more_columns [Array]
    #   Additional columns to unnest, specified as positional arguments.
    # @param separator [String]
    #   Rename output column names as combination of the struct column name,
    #   name separator and field name.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "before" => ["foo", "bar"],
    #       "t_a" => [1, 2],
    #       "t_b" => ["a", "b"],
    #       "t_c" => [true, nil],
    #       "t_d" => [[1, 2], [3]],
    #       "after" => ["baz", "womp"]
    #     }
    #   ).select(["before", Polars.struct(Polars.col("^t_.$")).alias("t_struct"), "after"])
    #   df.unnest("t_struct")
    #   # =>
    #   # shape: (2, 6)
    #   # ┌────────┬─────┬─────┬──────┬───────────┬───────┐
    #   # │ before ┆ t_a ┆ t_b ┆ t_c  ┆ t_d       ┆ after │
    #   # │ ---    ┆ --- ┆ --- ┆ ---  ┆ ---       ┆ ---   │
    #   # │ str    ┆ i64 ┆ str ┆ bool ┆ list[i64] ┆ str   │
    #   # ╞════════╪═════╪═════╪══════╪═══════════╪═══════╡
    #   # │ foo    ┆ 1   ┆ a   ┆ true ┆ [1, 2]    ┆ baz   │
    #   # │ bar    ┆ 2   ┆ b   ┆ null ┆ [3]       ┆ womp  │
    #   # └────────┴─────┴─────┴──────┴───────────┴───────┘
    def unnest(columns, *more_columns, separator: nil)
      lazy.unnest(columns, *more_columns, separator: separator).collect(_eager: true)
    end

    # Requires NumPy
    # def corr
    # end

    # Take two sorted DataFrames and merge them by the sorted key.
    #
    # The output of this operation will also be sorted.
    # It is the callers responsibility that the frames are sorted
    # by that key otherwise the output will not make sense.
    #
    # The schemas of both DataFrames must be equal.
    #
    # @param other [DataFrame]
    #   Other DataFrame that must be merged
    # @param key [String]
    #   Key that is sorted.
    #
    # @return [DataFrame]
    #
    # @example
    #   df0 = Polars::DataFrame.new(
    #     {"name" => ["steve", "elise", "bob"], "age" => [42, 44, 18]}
    #   ).sort("age")
    #   df1 = Polars::DataFrame.new(
    #     {"name" => ["anna", "megan", "steve", "thomas"], "age" => [21, 33, 42, 20]}
    #   ).sort("age")
    #   df0.merge_sorted(df1, "age")
    #   # =>
    #   # shape: (7, 2)
    #   # ┌────────┬─────┐
    #   # │ name   ┆ age │
    #   # │ ---    ┆ --- │
    #   # │ str    ┆ i64 │
    #   # ╞════════╪═════╡
    #   # │ bob    ┆ 18  │
    #   # │ thomas ┆ 20  │
    #   # │ anna   ┆ 21  │
    #   # │ megan  ┆ 33  │
    #   # │ steve  ┆ 42  │
    #   # │ steve  ┆ 42  │
    #   # │ elise  ┆ 44  │
    #   # └────────┴─────┘
    def merge_sorted(other, key)
      lazy.merge_sorted(other.lazy, key).collect(_eager: true)
    end

    # Flag a column as sorted.
    #
    # This can speed up future operations.
    #
    # @note
    #   This can lead to incorrect results if the data is NOT sorted! Use with care!
    #
    # @param column [Object]
    #   Column that is sorted.
    # @param descending [Boolean]
    #   Whether the column is sorted in descending order.
    #
    # @return [DataFrame]
    def set_sorted(
      column,
      descending: false
    )
      lazy
        .set_sorted(column, descending: descending)
        .collect(no_optimization: true)
    end

    # Update the values in this `DataFrame` with the values in `other`.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param other [DataFrame]
    #   DataFrame that will be used to update the values
    # @param on [Object]
    #   Column names that will be joined on. If set to `nil` (default),
    #   the implicit row index of each frame is used as a join key.
    # @param how ['left', 'inner', 'full']
    #   * 'left' will keep all rows from the left table; rows may be duplicated
    #     if multiple rows in the right frame match the left row's key.
    #   * 'inner' keeps only those rows where the key exists in both frames.
    #   * 'full' will update existing rows where the key matches while also
    #     adding any new rows contained in the given frame.
    # @param left_on [Object]
    #   Join column(s) of the left DataFrame.
    # @param right_on [Object]
    #   Join column(s) of the right DataFrame.
    # @param include_nulls [Boolean]
    #   Overwrite values in the left frame with null values from the right frame.
    #   If set to `false` (default), null values in the right frame are ignored.
    # @param maintain_order ['none', 'left', 'right', 'left_right', 'right_left']
    #   Which order of rows from the inputs to preserve. See `DataFrame.join`
    #   for details. Unlike `join` this function preserves the left order by
    #   default.
    #
    # @return [DataFrame]
    #
    # @note
    #   This is syntactic sugar for a left/inner join that preserves the order
    #   of the left `DataFrame` by default, with an optional coalesce when
    #   `include_nulls: false`.
    #
    # @example Update `df` values with the non-null values in `new_df`, by row index:
    #   df = Polars::DataFrame.new(
    #     {
    #       "A" => [1, 2, 3, 4],
    #       "B" => [400, 500, 600, 700]
    #     }
    #   )
    #   new_df = Polars::DataFrame.new(
    #     {
    #       "B" => [-66, nil, -99],
    #       "C" => [5, 3, 1]
    #     }
    #   )
    #   df.update(new_df)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬─────┐
    #   # │ A   ┆ B   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ -66 │
    #   # │ 2   ┆ 500 │
    #   # │ 3   ┆ -99 │
    #   # │ 4   ┆ 700 │
    #   # └─────┴─────┘
    #
    # @example Update `df` values with the non-null values in `new_df`, by row index, but only keeping those rows that are common to both frames:
    #   df.update(new_df, how: "inner")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ A   ┆ B   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ -66 │
    #   # │ 2   ┆ 500 │
    #   # │ 3   ┆ -99 │
    #   # └─────┴─────┘
    #
    # @example Update `df` values with the non-null values in `new_df`, using a full outer join strategy that defines explicit join columns in each frame:
    #   df.update(new_df, left_on: ["A"], right_on: ["C"], how: "full")
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬─────┐
    #   # │ A   ┆ B   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ -99 │
    #   # │ 2   ┆ 500 │
    #   # │ 3   ┆ 600 │
    #   # │ 4   ┆ 700 │
    #   # │ 5   ┆ -66 │
    #   # └─────┴─────┘
    #
    # @example Update `df` values including null values in `new_df`, using a full outer join strategy that defines explicit join columns in each frame:
    #   df.update(new_df, left_on: "A", right_on: "C", how: "full", include_nulls: true)
    #   # =>
    #   # shape: (5, 2)
    #   # ┌─────┬──────┐
    #   # │ A   ┆ B    │
    #   # │ --- ┆ ---  │
    #   # │ i64 ┆ i64  │
    #   # ╞═════╪══════╡
    #   # │ 1   ┆ -99  │
    #   # │ 2   ┆ 500  │
    #   # │ 3   ┆ null │
    #   # │ 4   ┆ 700  │
    #   # │ 5   ┆ -66  │
    #   # └─────┴──────┘
    def update(
      other,
      on: nil,
      how: "left",
      left_on: nil,
      right_on: nil,
      include_nulls: false,
      maintain_order: "left"
    )
      Utils.require_same_type(self, other)
      lazy
      .update(
        other.lazy,
        on: on,
        how: how,
        left_on: left_on,
        right_on: right_on,
        include_nulls: include_nulls,
        maintain_order: maintain_order
      )
      .collect(_eager: true)
    end

    private

    def initialize_copy(other)
      super
      self._df = _df._clone
    end

    def _pos_idx(idx, dim)
      if idx >= 0
        idx
      else
        shape[dim] + idx
      end
    end

    def _pos_idxs(idxs, dim)
      idx_type = Plr.get_index_type

      if idxs.is_a?(Series)
        if idxs.dtype == idx_type
          return idxs
        end
        if [UInt8, UInt16, idx_type == UInt32 ? UInt64 : UInt32, Int8, Int16, Int32, Int64].include?(idxs.dtype)
          if idx_type == UInt32
            if [Int64, UInt64].include?(idxs.dtype)
              if idxs.max >= 2**32
                raise ArgumentError, "Index positions should be smaller than 2^32."
              end
            end
            if idxs.dtype == Int64
              if idxs.min < -(2**32)
                raise ArgumentError, "Index positions should be bigger than -2^32 + 1."
              end
            end
          end
          if [Int8, Int16, Int32, Int64].include?(idxs.dtype)
            if idxs.min < 0
              if idx_type == UInt32
                if [Int8, Int16].include?(idxs.dtype)
                  idxs = idxs.cast(Int32)
                end
              else
                if [Int8, Int16, Int32].include?(idxs.dtype)
                  idxs = idxs.cast(Int64)
                end
              end

              idxs =
                Polars.select(
                  Polars.when(Polars.lit(idxs) < 0)
                    .then(shape[dim] + Polars.lit(idxs))
                    .otherwise(Polars.lit(idxs))
                ).to_series
            end
          end

          return idxs.cast(idx_type)
        end
      end

      raise ArgumentError, "Unsupported idxs datatype."
    end

    # @private
    def self._expand_hash_values(data, schema_overrides: nil, strict: true, order: nil, nan_to_null: false)
      updated_data = {}
      unless data.empty?
        dtypes = schema_overrides || {}
        array_len = data.values.map { |val| Utils.arrlen(val) || 0 }.max
        if array_len > 0
          data.each do |name, val|
            dtype = dtypes[name]
            if val.is_a?(Hash) && dtype != Struct
              updated_data[name] = DataFrame.new(val, strict: strict).to_struct(name)
            elsif !Utils.arrlen(val).nil?
              updated_data[name] = Series.new(::String.new(name), val, dtype: dtype, strict: strict)
            elsif val.nil? || [Integer, Float, TrueClass, FalseClass, ::String, ::Date, ::DateTime, ::Time].any? { |cls| val.is_a?(cls) }
              dtype = Polars::Float64 if val.nil? && dtype.nil?
              updated_data[name] = Series.new(::String.new(name), [val], dtype: dtype, strict: strict).extend_constant(val, array_len - 1)
            else
              raise Todo
            end
          end
        elsif data.values.all? { |val| Utils.arrlen(val) == 0 }
          data.each do |name, val|
            updated_data[name] = Series.new(name, val, dtype: dtypes[name], strict: strict)
          end
        elsif data.values.all? { |val| Utils.arrlen(val).nil? }
          data.each do |name, val|
            updated_data[name] = Series.new(name, [val], dtype: dtypes[name], strict: strict)
          end
        end
      end
      updated_data
    end

    # @private
    def self.hash_to_rbdf(data, schema: nil, schema_overrides: nil, strict: true, nan_to_null: nil)
      if schema.is_a?(Hash) && !data.empty?
        if !data.all? { |col, _| schema[col] }
          raise ArgumentError, "The given column-schema names do not match the data dictionary"
        end

        data = schema.to_h { |col| [col, data[col]] }
      end

      column_names, schema_overrides = _unpack_schema(
        schema, lookup_names: data.keys, schema_overrides: schema_overrides
      )
      if column_names.empty?
        column_names = data.keys
      end

      if data.empty? && !schema_overrides.empty?
        data_series = column_names.map { |name| Series.new(name, [], dtype: schema_overrides[name], strict: strict, nan_to_null: nan_to_null)._s }
      else
        data_series = _expand_hash_values(data, schema_overrides: schema_overrides, strict: strict, nan_to_null: nan_to_null).values.map(&:_s)
      end

      data_series = _handle_columns_arg(data_series, columns: column_names, from_hash: true)
      RbDataFrame.new(data_series)
    end

    # @private
    def self._unpack_schema(schema, schema_overrides: nil, n_expected: nil, lookup_names: nil, include_overrides_in_columns: false)
      if schema.is_a?(Hash)
        schema = schema.to_a
      end
      column_names =
        (schema || []).map.with_index do |col, i|
          if col.is_a?(::String)
            col || "column_#{i}"
          else
            col[0]
          end
        end
      if column_names.empty? && n_expected
        column_names = n_expected.times.map { |i| "column_#{i}" }
      end
      # TODO zip_longest
      lookup = column_names.zip(lookup_names || []).to_h

      column_dtypes =
        (schema || []).select { |col| !col.is_a?(::String) && col[1] }.to_h do |col|
          [lookup[col[0]] || col[0], col[1]]
        end

      if schema_overrides && schema_overrides.any?
        column_dtypes.merge!(schema_overrides)
      end

      column_dtypes.each do |col, dtype|
        if !Utils.is_polars_dtype(dtype, include_unknown: true) && !dtype.nil?
          column_dtypes[col] = Utils.rb_type_to_dtype(dtype)
        end
      end

      [column_names, column_dtypes]
    end

    def self._handle_columns_arg(data, columns: nil, from_hash: false)
      if columns.nil? || columns.empty?
        data
      else
        if data.empty?
          columns.map { |c| Series.new(c, nil)._s }
        elsif data.length == columns.length
          if from_hash
            series_map = data.to_h { |s| [s.name, s] }
            if columns.all? { |col| series_map.key?(col) }
              return columns.map { |col| series_map[col] }
            end
          end

          columns.each_with_index do |c, i|
            # not in-place?
            data[i].rename(c)
          end
          data
        else
          raise ArgumentError, "Dimensions of columns arg must match data dimensions."
        end
      end
    end

    def self._post_apply_columns(rbdf, columns, structs: nil, schema_overrides: nil, strict: true)
      rbdf_columns = rbdf.columns
      rbdf_dtypes = rbdf.dtypes
      columns, dtypes = _unpack_schema(
        (columns || rbdf_columns), schema_overrides: schema_overrides
      )
      column_subset = []
      if columns != rbdf_columns
        if columns.length < rbdf_columns.length && columns == rbdf_columns.first(columns.length)
          column_subset = columns
        else
          rbdf.set_column_names(columns)
        end
      end

      column_casts = []
      columns.each_with_index do |col, i|
        if dtypes[col] == Categorical # != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(Categorical, strict: strict)._rbexpr
        elsif structs&.any? && structs.include?(col) && structs[col] != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(structs[col], strict: strict)._rbexpr
        elsif dtypes.include?(col) && dtypes[col] != rbdf_dtypes[i]
          column_casts << Polars.col(col).cast(dtypes[col], strict: strict)._rbexpr
        end
      end

      if column_casts.any? || column_subset.any?
        rbdf = rbdf.lazy
        if column_casts.any?
          rbdf = rbdf.with_columns(column_casts)
        end
        if column_subset.any?
          rbdf = rbdf.select(column_subset.map { |col| Polars.col(col)._rbexpr })
        end
        rbdf = rbdf.collect
      end

      rbdf
    end

    # @private
    def self.sequence_to_rbdf(data, schema: nil, schema_overrides: nil, strict: true, orient: nil, infer_schema_length: 50)
      columns = schema

      if data.length == 0
        return hash_to_rbdf({}, schema: schema, schema_overrides: schema_overrides, strict: strict)
      end

      if data[0].is_a?(Series)
        # series_names = data.map(&:name)
        # columns, dtypes = _unpack_schema(columns || series_names, n_expected: data.length)
        data_series = []
        data.each do |s|
          data_series << s._s
        end
      elsif data[0].is_a?(Hash)
        column_names, dtypes = _unpack_schema(columns)
        schema_overrides = dtypes ? _include_unknowns(dtypes, column_names) : nil
        rbdf = RbDataFrame.from_hashes(data, schema, schema_overrides, strict, infer_schema_length)
        if column_names
          rbdf = _post_apply_columns(rbdf, column_names)
        end
        return rbdf
      elsif data[0].is_a?(::Array)
        first_element = data[0]
        if orient.nil? && !columns.nil?
          row_types = first_element.filter_map { |value| value.class }.uniq
          if row_types.include?(Integer) && row_types.include?(Float)
            row_types.delete(Integer)
          end
          orient = row_types.length == 1 ? "col" : "row"
        end

        if orient == "row"
          column_names, schema_overrides = _unpack_schema(
            schema, schema_overrides: schema_overrides, n_expected: first_element.length
          )
          local_schema_override = (
            schema_overrides.any? ? _include_unknowns(schema_overrides, column_names) : {}
          )
          if column_names.any? && first_element.length > 0 && first_element.length != column_names.length
            raise ArgumentError, "the row data does not match the number of columns"
          end

          unpack_nested = false
          local_schema_override.each do |col, tp|
            if [Categorical, Enum].include?(tp)
              local_schema_override[col] = String
            elsif !unpack_nested && [Unknown, Struct].include?(tp.base_type)
              raise Todo
            end
          end

          if unpack_nested
            raise Todo
          else
            rbdf = RbDataFrame.from_rows(
              data,
              infer_schema_length,
              local_schema_override.any? ? local_schema_override : nil
            )
          end
          if column_names.any? || schema_overrides.any?
            rbdf = _post_apply_columns(
              rbdf, column_names, schema_overrides: schema_overrides, strict: strict
            )
          end
          return rbdf
        elsif orient == "col" || orient.nil?
          column_names, schema_overrides = _unpack_schema(
            schema, schema_overrides: schema_overrides, n_expected: data.length
          )
          data_series =
            data.map.with_index do |element, i|
              Series.new(column_names[i], element, dtype: schema_overrides[column_names[i]], strict: strict)._s
            end
          return RbDataFrame.new(data_series)
        else
          raise ArgumentError, "orient must be one of {{'col', 'row', nil}}, got #{orient} instead."
        end
      end

      data_series = _handle_columns_arg(data_series, columns: columns)
      RbDataFrame.new(data_series)
    end

    # @private
    def self._include_unknowns(schema, cols)
      cols.to_h { |col| [col, schema[col] || Unknown] }
    end

    # @private
    def self.series_to_rbdf(data, schema: nil, schema_overrides: nil, strict: true)
      data_series = [data._s]
      series_name = data_series.map(&:name)
      column_names, schema_overrides = _unpack_schema(
        schema || series_name, schema_overrides: schema_overrides, n_expected: 1
      )
      if schema_overrides.any?
        new_dtype = schema_overrides.values[0]
        if new_dtype != data.dtype
          data_series[0] = data_series[0].cast(new_dtype, strict)
        end
      end

      data_series = _handle_columns_arg(data_series, columns: column_names)
      RbDataFrame.new(data_series)
    end

    def wrap_ldf(ldf)
      LazyFrame._from_rbldf(ldf)
    end

    def _from_rbdf(rb_df)
      self.class._from_rbdf(rb_df)
    end

    def _replace(column, new_column)
      self._df.replace(column, new_column._s)
      self
    end

    def _comp(other, op)
      if other.is_a?(DataFrame)
        _compare_to_other_df(other, op)
      else
        _compare_to_non_df(other, op)
      end
    end

    def _compare_to_other_df(other, op)
      if columns != other.columns
        raise ArgumentError, "DataFrame columns do not match"
      end
      if shape != other.shape
        raise ArgumentError, "DataFrame dimensions do not match"
      end

      suffix = "__POLARS_CMP_OTHER"
      other_renamed = other.select(Polars.all.name.suffix(suffix))
      combined = Polars.concat([self, other_renamed], how: "horizontal")

      expr = case op
      when "eq"
        columns.map { |n| Polars.col(n) == Polars.col("#{n}#{suffix}") }
      when "neq"
        columns.map { |n| Polars.col(n) != Polars.col("#{n}#{suffix}") }
      when "gt"
        columns.map { |n| Polars.col(n) > Polars.col("#{n}#{suffix}") }
      when "lt"
        columns.map { |n| Polars.col(n) < Polars.col("#{n}#{suffix}") }
      when "gt_eq"
        columns.map { |n| Polars.col(n) >= Polars.col("#{n}#{suffix}") }
      when "lt_eq"
        columns.map { |n| Polars.col(n) <= Polars.col("#{n}#{suffix}") }
      else
        raise ArgumentError, "got unexpected comparison operator: #{op}"
      end

      combined.select(expr)
    end

    def _compare_to_non_df(other, op)
      case op
      when "eq"
        select(Polars.all == other)
      when "neq"
        select(Polars.all != other)
      when "gt"
        select(Polars.all > other)
      when "lt"
        select(Polars.all < other)
      when "gt_eq"
        select(Polars.all >= other)
      when "lt_eq"
        select(Polars.all <= other)
      else
        raise ArgumentError, "got unexpected comparison operator: #{op}"
      end
    end

    def _prepare_other_arg(other)
      if !other.is_a?(Series)
        if other.is_a?(::Array)
          raise ArgumentError, "Operation not supported."
        end

        other = Series.new("", [other])
      end
      other
    end

    def with_connection(connection, &block)
      if !connection.nil?
        yield connection
      else
        ActiveRecord::Base.connection_pool.with_connection(&block)
      end
    end

    def maybe_transaction(connection, create_table, &block)
      if create_table && connection.adapter_name.match?(/postg|sqlite/i) && connection.open_transactions == 0
        connection.transaction(&block)
      else
        yield
      end
    end
  end
end

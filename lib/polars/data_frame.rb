module Polars
  # Two-dimensional data structure representing data as a table with rows and columns.
  class DataFrame
    # @private
    attr_accessor :_df

    # Create a new DataFrame.
    #
    # @param data [Hash, Array, Series, nil]
    #   Two-dimensional data in various forms. Hash must contain Arrays.
    #   Array may contain Series.
    # @param columns [Array, Hash, nil]
    #   Column labels to use for resulting DataFrame. If specified, overrides any
    #   labels already present in the data. Must match data dimensions.
    # @param orient ["col", "row", nil]
    #   Whether to interpret two-dimensional data as columns or as rows. If `nil`,
    #   the orientation is inferred by matching the columns and data dimensions. If
    #   this does not yield conclusive results, column orientation is used.
    def initialize(data = nil, columns: nil, orient: nil)
      if defined?(ActiveRecord) && (data.is_a?(ActiveRecord::Relation) || data.is_a?(ActiveRecord::Result))
        result = data.is_a?(ActiveRecord::Result) ? data : data.connection.select_all(data.to_sql)
        data = {}
        result.columns.each_with_index do |k, i|
          data[k] = result.rows.map { |r| r[i] }
        end
      end

      if data.nil?
        self._df = hash_to_rbdf({}, columns: columns)
      elsif data.is_a?(Hash)
        data = data.transform_keys { |v| v.is_a?(Symbol) ? v.to_s : v }
        self._df = hash_to_rbdf(data, columns: columns)
      elsif data.is_a?(Array)
        self._df = sequence_to_rbdf(data, columns: columns, orient: orient)
      elsif data.is_a?(Series)
        self._df = series_to_rbdf(data, columns: columns)
      else
        raise ArgumentError, "DataFrame constructor called with unsupported type; got #{data.class.name}"
      end
    end

    # @private
    def self._from_rbdf(rb_df)
      df = DataFrame.allocate
      df._df = rb_df
      df
    end

    # def self._from_hashes
    # end

    # def self._from_hash
    # end

    # def self._from_records
    # end

    # def self._from_numo
    # end

    # no self._from_arrow

    # no self._from_pandas

    # @private
    def self._read_csv(
      file,
      has_header: true,
      columns: nil,
      sep: str = ",",
      comment_char: nil,
      quote_char: '"',
      skip_rows: 0,
      dtypes: nil,
      null_values: nil,
      ignore_errors: false,
      parse_dates: false,
      n_threads: nil,
      infer_schema_length: 100,
      batch_size: 8192,
      n_rows: nil,
      encoding: "utf8",
      low_memory: false,
      rechunk: true,
      skip_rows_after_header: 0,
      row_count_name: nil,
      row_count_offset: 0,
      sample_size: 1024,
      eol_char: "\n"
    )
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        path = Utils.format_path(file)
      else
        path = nil
        # if defined?(StringIO) && file.is_a?(StringIO)
        #   file = file.string
        # end
      end

      dtype_list = nil
      dtype_slice = nil
      if !dtypes.nil?
        if dtypes.is_a?(Hash)
          dtype_list = []
          dtypes.each do|k, v|
            dtype_list << [k, Utils.rb_type_to_dtype(v)]
          end
        elsif dtypes.is_a?(Array)
          dtype_slice = dtypes
        else
          raise ArgumentError, "dtype arg should be list or dict"
        end
      end

      processed_null_values = Utils._process_null_values(null_values)

      if columns.is_a?(String)
        columns = [columns]
      end
      if file.is_a?(String) && file.include?("*")
        raise Todo
      end

      projection, columns = Utils.handle_projection_columns(columns)

      _from_rbdf(
        RbDataFrame.read_csv(
          file,
          infer_schema_length,
          batch_size,
          has_header,
          ignore_errors,
          n_rows,
          skip_rows,
          projection,
          sep,
          rechunk,
          columns,
          encoding,
          n_threads,
          path,
          dtype_list,
          dtype_slice,
          low_memory,
          comment_char,
          quote_char,
          processed_null_values,
          parse_dates,
          skip_rows_after_header,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          sample_size,
          eol_char
        )
      )
    end

    # @private
    def self._read_parquet(
      file,
      columns: nil,
      n_rows: nil,
      parallel: "auto",
      row_count_name: nil,
      row_count_offset: 0,
      low_memory: false
    )
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      if file.is_a?(String) && file.include?("*")
        raise Todo
      end

      projection, columns = Utils.handle_projection_columns(columns)
      _from_rbdf(
        RbDataFrame.read_parquet(
          file,
          columns,
          projection,
          n_rows,
          parallel,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          low_memory
        )
      )
    end

    # def self._read_avro
    # end

    # @private
    def self._read_ipc(
      file,
      columns: nil,
      n_rows: nil,
      row_count_name: nil,
      row_count_offset: 0,
      rechunk: true,
      memory_map: true
    )
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end
      if columns.is_a?(String)
        columns = [columns]
      end

      if file.is_a?(String) && file.include?("*")
        raise Todo
      end

      projection, columns = Utils.handle_projection_columns(columns)
      _from_rbdf(
        RbDataFrame.read_ipc(
          file,
          columns,
          projection,
          n_rows,
          Utils._prepare_row_count_args(row_count_name, row_count_offset),
          memory_map
        )
      )
    end

    # @private
    def self._read_json(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_json(file))
    end

    # @private
    def self._read_ndjson(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_ndjson(file))
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
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
    #   # │ 2     ┆ 7      ┆ b      │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
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
    #   # => [:i64, :f64, :str]
    def dtypes
      _df.dtypes
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
    #   # => {"foo"=>:i64, "bar"=>:f64, "ham"=>:str}
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

    # Check if DataFrame includes column.
    #
    # @return [Boolean]
    def include?(name)
      columns.include?(name)
    end

    # def each
    # end

    # def _pos_idx
    # end

    # def _pos_idxs
    # end

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
          if col_selection.is_a?(Array)
            df = self[0.., col_selection]
            return df.slice(row_selection, 1)
          end
          # df[2, "a"]
          if col_selection.is_a?(String)
            return self[col_selection][row_selection]
          end
        end

        # column selection can be "a" and ["a", "b"]
        if col_selection.is_a?(String)
          col_selection = [col_selection]
        end

        # df[.., 1]
        if col_selection.is_a?(Integer)
          series = to_series(col_selection)
          return series[row_selection]
        end

        if col_selection.is_a?(Array)
          # df[.., [1, 2]]
          if is_int_sequence(col_selection)
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
        if item.is_a?(String)
          return Utils.wrap_s(_df.column(item))
        end

        # df[idx]
        if item.is_a?(Integer)
          return slice(_pos_idx(item, dim: 0), 1)
        end

        # df[..]
        if item.is_a?(Range)
          return Slice.new(self).apply(item)
        end
      end

      raise ArgumentError, "Cannot get item of type: #{item.class.name}"
    end

    # def []=(key, value)
    # end

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

    # def to_hashes / to_a
    # end

    # def to_numo
    # end

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

    # Serialize to JSON representation.
    #
    # @return [nil]
    #
    # @param file [String]
    #   File path to which the result should be written.
    # @param pretty [Boolean]
    #   Pretty serialize json.
    # @param row_oriented [Boolean]
    #   Write to row oriented json. This is slower, but more common.
    #
    # @see #write_ndjson
    def write_json(
      file,
      pretty: false,
      row_oriented: false
    )
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _df.write_json(file, pretty, row_oriented)
      nil
    end

    # Serialize to newline delimited JSON representation.
    #
    # @param file [String]
    #   File path to which the result should be written.
    #
    # @return [nil]
    def write_ndjson(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _df.write_ndjson(file)
      nil
    end

    # Write to comma-separated values (CSV) file.
    #
    # @param file [String, nil]
    #   File path to which the result should be written. If set to `nil`
    #   (default), the output is returned as a string instead.
    # @param has_header [Boolean]
    #   Whether to include header in the CSV output.
    # @param sep [String]
    #   Separate CSV fields with this symbol.
    # @param quote [String]
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
      has_header: true,
      sep: ",",
      quote: '"',
      batch_size: 1024,
      datetime_format: nil,
      date_format: nil,
      time_format: nil,
      float_precision: nil,
      null_value: nil
    )
      if sep.length > 1
        raise ArgumentError, "only single byte separator is allowed"
      elsif quote.length > 1
        raise ArgumentError, "only single byte quote char is allowed"
      elsif null_value == ""
        null_value = nil
      end

      if file.nil?
        buffer = StringIO.new
        buffer.set_encoding(Encoding::BINARY)
        _df.write_csv(
          buffer,
          has_header,
          sep.ord,
          quote.ord,
          batch_size,
          datetime_format,
          date_format,
          time_format,
          float_precision,
          null_value
        )
        return buffer.string.force_encoding(Encoding::UTF_8)
      end

      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _df.write_csv(
        file,
        has_header,
        sep.ord,
        quote.ord,
        batch_size,
        datetime_format,
        date_format,
        time_format,
        float_precision,
        null_value,
      )
      nil
    end

    # def write_avro
    # end

    # Write to Arrow IPC binary stream or Feather file.
    #
    # @param file [String]
    #   File path to which the file should be written.
    # @param compression ["uncompressed", "lz4", "zstd"]
    #   Compression method. Defaults to "uncompressed".
    #
    # @return [nil]
    def write_ipc(file, compression: "uncompressed")
      if compression.nil?
        compression = "uncompressed"
      end
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _df.write_ipc(file, compression)
    end

    # Write to Apache Parquet file.
    #
    # @param file [String]
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
    #   Size of the row groups in number of rows.
    #   If `nil` (default), the chunks of the DataFrame are
    #   used. Writing in smaller chunks may reduce memory pressure and improve
    #   writing speeds.
    #
    # @return [nil]
    def write_parquet(
      file,
      compression: "zstd",
      compression_level: nil,
      statistics: false,
      row_group_size: nil
    )
      if compression.nil?
        compression = "uncompressed"
      end
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _df.write_parquet(
        file, compression, compression_level, statistics, row_group_size
      )
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
    #     columns: {"x" => :u32, "y" => :f64, "z" => :str}
    #   )
    #   df.estimated_size
    #   # => 25888898
    #   df.estimated_size("mb")
    #   # => 24.689577102661133
    def estimated_size(unit = "b")
      sz = _df.estimated_size
      Utils.scale_bytes(sz, to: unit)
    end

    # def transpose
    # end

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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b   ┆ 2   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ a   ┆ 1   │
    #   # └─────┴─────┘
    def reverse
      select(Polars.col("*").reverse)
    end

    # Rename column names.
    #
    # @param mapping [Hash]
    #   Key value pairs that map from old name to new name.
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
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2     ┆ 7   ┆ b   │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3     ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    def rename(mapping)
      lazy.rename(mapping).collect(no_optimization: true)
    end

    # Insert a Series at a certain column index. This operation is in place.
    #
    # @param index [Integer]
    #   Column to insert the new `Series` column.
    # @param series [Series]
    #   `Series` to insert.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   s = Polars::Series.new("baz", [97, 98, 99])
    #   df.insert_at_idx(1, s)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ baz ┆ bar │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 97  ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 98  ┆ 5   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   df.insert_at_idx(3, s)
    #   # =>
    #   # shape: (4, 4)
    #   # ┌─────┬──────┬───────┬──────┐
    #   # │ a   ┆ b    ┆ c     ┆ d    │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  │
    #   # ╞═════╪══════╪═══════╪══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ -2.5 │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 15.0 │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 3   ┆ 10.0 ┆ false ┆ 20.5 │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 0.0  │
    #   # └─────┴──────┴───────┴──────┘
    def insert_at_idx(index, series)
      if index < 0
        index = columns.length + index
      end
      _df.insert_at_idx(index, series._s)
      self
    end

    # Filter the rows in the DataFrame based on a predicate expression.
    #
    # @param predicate [Expr]
    #   Expression that evaluates to a boolean Series.
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
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
    def filter(predicate)
      lazy.filter(predicate).collect
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
    #   # ┌────────────┬──────────┬──────────┬──────┬──────┬──────┐
    #   # │ describe   ┆ a        ┆ b        ┆ c    ┆ d    ┆ e    │
    #   # │ ---        ┆ ---      ┆ ---      ┆ ---  ┆ ---  ┆ ---  │
    #   # │ str        ┆ f64      ┆ f64      ┆ f64  ┆ str  ┆ str  │
    #   # ╞════════════╪══════════╪══════════╪══════╪══════╪══════╡
    #   # │ count      ┆ 3.0      ┆ 3.0      ┆ 3.0  ┆ 3    ┆ 3    │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ null_count ┆ 0.0      ┆ 1.0      ┆ 0.0  ┆ 1    ┆ 1    │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ mean       ┆ 2.266667 ┆ 4.5      ┆ null ┆ null ┆ null │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ std        ┆ 1.101514 ┆ 0.707107 ┆ null ┆ null ┆ null │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ min        ┆ 1.0      ┆ 4.0      ┆ 0.0  ┆ b    ┆ eur  │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ max        ┆ 3.0      ┆ 5.0      ┆ 1.0  ┆ c    ┆ usd  │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ median     ┆ 2.8      ┆ 4.5      ┆ null ┆ null ┆ null │
    #   # └────────────┴──────────┴──────────┴──────┴──────┴──────┘
    def describe
      describe_cast = lambda do |stat|
        columns = []
        self.columns.each_with_index do |s, i|
          if self[s].is_numeric || self[s].is_boolean
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
      summary.insert_at_idx(
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
    #   df.find_idx_by_name("ham")
    #   # => 2
    def find_idx_by_name(name)
      _df.find_idx_by_name(name)
    end

    # Replace a column at an index location.
    #
    # @param index [Integer]
    #   Column index.
    # @param series [Series]
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
    #   df.replace_at_idx(0, s)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬─────┐
    #   # │ apple ┆ bar ┆ ham │
    #   # │ ---   ┆ --- ┆ --- │
    #   # │ i64   ┆ i64 ┆ str │
    #   # ╞═══════╪═════╪═════╡
    #   # │ 10    ┆ 6   ┆ a   │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 20    ┆ 7   ┆ b   │
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 30    ┆ 8   ┆ c   │
    #   # └───────┴─────┴─────┘
    def replace_at_idx(index, series)
      if index < 0
        index = columns.length + index
      end
      _df.replace_at_idx(index, series._s)
      self
    end

    # Sort the DataFrame by column.
    #
    # @param by [String]
    #   By which column to sort.
    # @param reverse [Boolean]
    #   Reverse/descending sort.
    # @param nulls_last [Boolean]
    #   Place null values last. Can only be used if sorted by a single column.
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
    #   df.sort("foo", reverse: true)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    #
    # @example Sort by multiple columns.
    #   df.sort(
    #     [Polars.col("foo"), Polars.col("bar")**2],
    #     reverse: [true, false]
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3   ┆ 8.0 ┆ c   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1   ┆ 6.0 ┆ a   │
    #   # └─────┴─────┴─────┘
    def sort(by, reverse: false, nulls_last: false)
      if by.is_a?(Array) || by.is_a?(Expr)
        lazy
          .sort(by, reverse: reverse, nulls_last: nulls_last)
          .collect(no_optimization: true, string_cache: false)
      else
        _from_rbdf(_df.sort(by, reverse, nulls_last))
      end
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
    #   df1.frame_equal(df1)
    #   # => true
    #   df1.frame_equal(df2)
    #   # => false
    def frame_equal(other, null_equal: true)
      _df.frame_equal(other._df, null_equal)
    end

    # Replace a column by a new Series.
    #
    # @param column [String]
    #   Column to replace.
    # @param new_col [Series]
    #   New column to insert.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, 2, 3], "bar" => [4, 5, 6]})
    #   s = Polars::Series.new([10, 20, 30])
    #   df.replace("foo", s)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 10  ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 20  ┆ 5   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 30  ┆ 6   │
    #   # └─────┴─────┘
    def replace(column, new_col)
      _df.replace(column, new_col._s)
      self
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ b   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ c   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   ┆ b   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 4   ┆ 9   ┆ d   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 5   ┆ 10  ┆ e   │
    #   # └─────┴─────┴─────┘
    def tail(n = 5)
      _from_rbdf(_df.tail(n))
    end

    # Return a new DataFrame where the null values are dropped.
    #
    # @param subset [Object]
    #   Subset of column(s) on which `drop_nulls` will be applied.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1, 2, 3],
    #       "bar" => [6, nil, 8],
    #       "ham" => ["a", "b", "c"]
    #     }
    #   )
    #   df.drop_nulls
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 1   ┆ 6   ┆ a   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 8   ┆ c   │
    #   # └─────┴─────┴─────┘
    def drop_nulls(subset: nil)
      if subset.is_a?(String)
        subset = [subset]
      end
      _from_rbdf(_df.drop_nulls(subset))
    end

    # def pipe
    # end

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
    #   df.with_row_count
    #   # =>
    #   # shape: (3, 3)
    #   # ┌────────┬─────┬─────┐
    #   # │ row_nr ┆ a   ┆ b   │
    #   # │ ---    ┆ --- ┆ --- │
    #   # │ u32    ┆ i64 ┆ i64 │
    #   # ╞════════╪═════╪═════╡
    #   # │ 0      ┆ 1   ┆ 2   │
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1      ┆ 3   ┆ 4   │
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2      ┆ 5   ┆ 6   │
    #   # └────────┴─────┴─────┘
    def with_row_count(name: "row_nr", offset: 0)
      _from_rbdf(_df.with_row_count(name, offset))
    end

    # Start a groupby operation.
    #
    # @param by [Object]
    #   Column(s) to group by.
    # @param maintain_order [Boolean]
    #   Make sure that the order of the groups remain consistent. This is more
    #   expensive than a default groupby. Note that this only works in expression
    #   aggregations.
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
    #   df.groupby("a").agg(Polars.col("b").sum).sort("a")
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ a   ┆ 4   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ b   ┆ 11  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ c   ┆ 6   │
    #   # └─────┴─────┘
    def groupby(by, maintain_order: false)
      if !Utils.bool?(maintain_order)
        raise TypeError, "invalid input for groupby arg `maintain_order`: #{maintain_order}."
      end
      if by.is_a?(String)
        by = [by]
      end
      GroupBy.new(
        _df,
        by,
        self.class,
        maintain_order: maintain_order
      )
    end

    # def groupby_rolling
    # end

    # def groupby_dynamic
    # end

    # def upsample
    # end

    # def join_asof
    # end

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
    # @param how ["inner", "left", "outer", "semi", "anti", "cross"]
    #   Join strategy.
    # @param suffix [String]
    #   Suffix to append to columns with a duplicate name.
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # └─────┴─────┴─────┴───────┘
    #
    # @example
    #   df.join(other_df, on: "ham", how: "outer")
    #   # =>
    #   # shape: (4, 4)
    #   # ┌──────┬──────┬─────┬───────┐
    #   # │ foo  ┆ bar  ┆ ham ┆ apple │
    #   # │ ---  ┆ ---  ┆ --- ┆ ---   │
    #   # │ i64  ┆ f64  ┆ str ┆ str   │
    #   # ╞══════╪══════╪═════╪═══════╡
    #   # │ 1    ┆ 6.0  ┆ a   ┆ x     │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2    ┆ 7.0  ┆ b   ┆ y     │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ null ┆ null ┆ d   ┆ z     │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3    ┆ 8.0  ┆ c   ┆ null  │
    #   # └──────┴──────┴─────┴───────┘
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 ┆ b   ┆ y     │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
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
    def join(other, left_on: nil, right_on: nil, on: nil, how: "inner", suffix: "_right")
      lazy
        .join(
          other.lazy,
          left_on: left_on,
          right_on: right_on,
          on: on,
          how: how,
          suffix: suffix,
        )
        .collect(no_optimization: true)
    end

    # def apply
    # end

    # Return a new DataFrame with the column added or replaced.
    #
    # @param column [Object]
    #   Series, where the name of the Series refers to the column in the DataFrame.
    #
    # @return [DataFrame]
    #
    # @example Added
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 3, 5],
    #       "b" => [2, 4, 6]
    #     }
    #   )
    #   df.with_column((Polars.col("b") ** 2).alias("b_squared"))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬───────────┐
    #   # │ a   ┆ b   ┆ b_squared │
    #   # │ --- ┆ --- ┆ ---       │
    #   # │ i64 ┆ i64 ┆ f64       │
    #   # ╞═════╪═════╪═══════════╡
    #   # │ 1   ┆ 2   ┆ 4.0       │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 4   ┆ 16.0      │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ 5   ┆ 6   ┆ 36.0      │
    #   # └─────┴─────┴───────────┘
    #
    # @example Replaced
    #   df.with_column(Polars.col("a") ** 2)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌──────┬─────┐
    #   # │ a    ┆ b   │
    #   # │ ---  ┆ --- │
    #   # │ f64  ┆ i64 │
    #   # ╞══════╪═════╡
    #   # │ 1.0  ┆ 2   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 9.0  ┆ 4   │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 25.0 ┆ 6   │
    #   # └──────┴─────┘
    def with_column(column)
      lazy
        .with_column(column)
        .collect(no_optimization: true, string_cache: false)
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 7   ┆ b   ┆ 20    │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 8   ┆ c   ┆ 30    │
    #   # └─────┴─────┴─────┴───────┘
    def hstack(columns, in_place: false)
      if !columns.is_a?(Array)
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
    # @param df [DataFrame]
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   ┆ b   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 8   ┆ c   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 4   ┆ 9   ┆ d   │
    #   # └─────┴─────┴─────┘
    def vstack(df, in_place: false)
      if in_place
        _df.vstack_mut(df._df)
        self
      else
        _from_rbdf(_df.vstack(df._df))
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 5   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 6   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 10  ┆ 40  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 20  ┆ 50  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7.0 │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 8.0 │
    #   # └─────┴─────┘
    def drop(columns)
      if columns.is_a?(Array)
        df = clone
        columns.each do |n|
          df._df.drop_in_place(n)
        end
        df
      else
        _from_rbdf(_df.drop(columns))
      end
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
    #   df.cleared
    #   # =>
    #   # shape: (0, 3)
    #   # ┌─────┬─────┬──────┐
    #   # │ a   ┆ b   ┆ c    │
    #   # │ --- ┆ --- ┆ ---  │
    #   # │ i64 ┆ f64 ┆ bool │
    #   # ╞═════╪═════╪══════╡
    #   # └─────┴─────┴──────┘
    def cleared
      height > 0 ? head(0) : clone
    end

    # clone handled by initialize_copy

    # Get the DataFrame as a Array of Series.
    #
    # @return [Array]
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

    # def fill_null
    # end

    # Fill floating point NaN values by an Expression evaluation.
    #
    # @param fill_value [Object]
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
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 2.0  ┆ 4.0  │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 99.0 ┆ 99.0 │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 4.0  ┆ 13.0 │
    #   # └──────┴──────┘
    def fill_nan(fill_value)
      lazy.fill_nan(fill_value).collect(no_optimization: true)
    end

    # Explode `DataFrame` to long format by exploding a column with Lists.
    #
    # @param columns [Object]
    #   Column of LargeList type.
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
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ a       ┆ 2       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ a       ┆ 3       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ b       ┆ 4       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ b       ┆ 5       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ c       ┆ 6       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ c       ┆ 7       │
    #   # ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
    #   # │ c       ┆ 8       │
    #   # └─────────┴─────────┘
    def explode(columns)
      lazy.explode(columns).collect(no_optimization: true)
    end

    # def pivot
    # end

    # def melt
    # end

    # def unstack
    # end

    # def partition_by
    # end

    # Shift values by the given period.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
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
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 1    ┆ 6    ┆ a    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ 3    ┆ 8    ┆ c    │
    #   # ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
    #   # │ null ┆ null ┆ null │
    #   # └──────┴──────┴──────┘
    def shift(periods)
      _from_rbdf(_df.shift(periods))
    end

    # Shift the values by a given period and fill the resulting null values.
    #
    # @param periods [Integer]
    #   Number of places to shift (may be negative).
    # @param fill_value [Object]
    #   fill nil values with this value.
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
    #   df.shift_and_fill(1, 0)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ ham │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ i64 ┆ str │
    #   # ╞═════╪═════╪═════╡
    #   # │ 0   ┆ 0   ┆ 0   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 1   ┆ 6   ┆ a   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
    def shift_and_fill(periods, fill_value)
      lazy
        .shift_and_fill(periods, fill_value)
        .collect(no_optimization: true, string_cache: false)
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
    # @param exprs [Object]
    #   Column or columns to select.
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
    #   # ├╌╌╌╌╌┤
    #   # │ 2   │
    #   # ├╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┤
    #   # │ 3   │
    #   # ├╌╌╌╌╌┤
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 8   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
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
    #   # │ i64     │
    #   # ╞═════════╡
    #   # │ 0       │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 0       │
    #   # ├╌╌╌╌╌╌╌╌╌┤
    #   # │ 10      │
    #   # └─────────┘
    def select(exprs)
      _from_rbdf(
        lazy
          .select(exprs)
          .collect(no_optimization: true, string_cache: false)
          ._df
      )
    end

    # Add or overwrite multiple columns in a DataFrame.
    #
    # @param exprs [Array]
    #   Array of Expressions that evaluate to columns.
    #
    # @return [DataFrame]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3, 4],
    #       "b" => [0.5, 4, 10, 13],
    #       "c" => [true, true, false, true]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       (Polars.col("a") ** 2).alias("a^2"),
    #       (Polars.col("b") / 2).alias("b/2"),
    #       (Polars.col("c").is_not).alias("not c")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 6)
    #   # ┌─────┬──────┬───────┬──────┬──────┬───────┐
    #   # │ a   ┆ b    ┆ c     ┆ a^2  ┆ b/2  ┆ not c │
    #   # │ --- ┆ ---  ┆ ---   ┆ ---  ┆ ---  ┆ ---   │
    #   # │ i64 ┆ f64  ┆ bool  ┆ f64  ┆ f64  ┆ bool  │
    #   # ╞═════╪══════╪═══════╪══════╪══════╪═══════╡
    #   # │ 1   ┆ 0.5  ┆ true  ┆ 1.0  ┆ 0.25 ┆ false │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 4.0  ┆ true  ┆ 4.0  ┆ 2.0  ┆ false │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 10.0 ┆ false ┆ 9.0  ┆ 5.0  ┆ true  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 4   ┆ 13.0 ┆ true  ┆ 16.0 ┆ 6.5  ┆ false │
    #   # └─────┴──────┴───────┴──────┴──────┴───────┘
    def with_columns(exprs)
      if !exprs.nil? && !exprs.is_a?(Array)
        exprs = [exprs]
      end
      lazy
        .with_columns(exprs)
        .collect(no_optimization: true, string_cache: false)
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
    def max(axis: 0)
      if axis == 0
        _from_rbdf(_df.max)
      elsif axis == 1
        Utils.wrap_s(_df.hmax)
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
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
    def min(axis: 0)
      if axis == 0
        _from_rbdf(_df.min)
      elsif axis == 1
        Utils.wrap_s(_df.hmin)
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    # Aggregate the columns of this DataFrame to their sum value.
    #
    # @param axis [Integer]
    #   Either 0 or 1.
    # @param null_strategy ["ignore", "propagate"]
    #   This argument is only used if axis == 1.
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
    #
    # @example
    #   df.sum(axis: 1)
    #   # =>
    #   # shape: (3,)
    #   # Series: 'foo' [str]
    #   # [
    #   #         "16a"
    #   #         "27b"
    #   #         "38c"
    #   # ]
    def sum(axis: 0, null_strategy: "ignore")
      case axis
      when 0
        _from_rbdf(_df.sum)
      when 1
        Utils.wrap_s(_df.hsum(null_strategy))
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    # Aggregate the columns of this DataFrame to their mean value.
    #
    # @param axis [Integer]
    #   Either 0 or 1.
    # @param null_strategy ["ignore", "propagate"]
    #   This argument is only used if axis == 1.
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
    def mean(axis: 0, null_strategy: "ignore")
      case axis
      when 0
        _from_rbdf(_df.mean)
      when 1
        Utils.wrap_s(_df.hmean(null_strategy))
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
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
      _from_rbdf(_df.std(ddof))
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
      _from_rbdf(_df.var(ddof))
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
      _from_rbdf(_df.median)
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
      _from_rbdf(_df.quantile(quantile, interpolation))
    end

    # Get one hot encoded dummy variables.
    #
    # @param columns
    #   A subset of columns to convert to dummy variables. `nil` means
    #   "all columns".
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
    #   # ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 0     ┆ 1     ┆ 0     ┆ 1     ┆ 0     ┆ 1     │
    #   # └───────┴───────┴───────┴───────┴───────┴───────┘
    def to_dummies(columns: nil)
      if columns.is_a?(String)
        columns = [columns]
      end
      _from_rbdf(_df.to_dummies(columns))
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
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 2   ┆ 1.0 ┆ true  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 3   ┆ 2.0 ┆ false │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 4   ┆ 3.0 ┆ true  │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ 5   ┆ 3.0 ┆ true  │
    #   # └─────┴─────┴───────┘
    def unique(maintain_order: true, subset: nil, keep: "first")
      if !subset.nil?
        if subset.is_a?(String)
          subset = [subset]
        elsif !subset.is_a?(Array)
          subset = subset.to_a
        end
      end

      _from_rbdf(_df.unique(maintain_order, subset, keep))
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

      if subset.is_a?(Array) && subset.length == 1
        expr = Utils.expr_to_lit_or_expr(subset[0], str_to_lit: false)
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
    #   Number of items to return. Cannot be used with `frac`. Defaults to 1 if
    #   `frac` is nil.
    # @param frac [Float]
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
    #   # │ 3   ┆ 8   ┆ c   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 2   ┆ 7   ┆ b   │
    #   # └─────┴─────┴─────┘
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
        _from_rbdf(
          _df.sample_frac(frac, with_replacement, shuffle, seed)
        )
      end

      if n.nil?
        n = 1
      end
      _from_rbdf(_df.sample_n(n, with_replacement, shuffle, seed))
    end

    # def fold
    # end

    # Get a row as tuple, either by index or by predicate.
    #
    # @param index [Object]
    #   Row index.
    # @param by_predicate [Object]
    #   Select the row according to a given expression/predicate.
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
    # @example Return the row that matches the given predicate
    #   df.row(by_predicate: Polars.col("ham") == "b")
    #   # => [2, 7, "b"]
    def row(index = nil, by_predicate: nil)
      if !index.nil? && !by_predicate.nil?
        raise ArgumentError, "Cannot set both 'index' and 'by_predicate'; mutually exclusive"
      elsif index.is_a?(Expr)
        raise TypeError, "Expressions should be passed to the 'by_predicate' param"
      elsif index.is_a?(Integer)
        _df.row_tuple(index)
      elsif by_predicate.is_a?(Expr)
        rows = filter(by_predicate).rows
        n_rows = rows.length
        if n_rows > 1
          raise TooManyRowsReturned, "Predicate #{by_predicate} returned #{n_rows} rows"
        elsif n_rows == 0
          raise NoRowsReturned, "Predicate <{by_predicate!s}> returned no rows"
        end
        rows[0]
      else
        raise ArgumentError, "One of 'index' or 'by_predicate' must be set"
      end
    end

    # Convert columnar data to rows as Ruby arrays.
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
    def rows
      _df.row_tuples
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
    #   s.take_every(2)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 5   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 3   ┆ 7   │
    #   # └─────┴─────┘
    def take_every(n)
      select(Utils.col("*").take_every(n))
    end

    # def hash_rows
    # end

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
    #   # ┌─────┬──────┬─────┐
    #   # │ foo ┆ bar  ┆ baz │
    #   # │ --- ┆ ---  ┆ --- │
    #   # │ i64 ┆ i64  ┆ i64 │
    #   # ╞═════╪══════╪═════╡
    #   # │ 1   ┆ 6    ┆ 1   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 5   ┆ 7    ┆ 3   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 9   ┆ 9    ┆ 6   │
    #   # ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌┤
    #   # │ 10  ┆ null ┆ 9   │
    #   # └─────┴──────┴─────┘
    def interpolate
      select(Utils.col("*").interpolate)
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
    # @param names [Object]
    #  Names of the struct columns that will be decomposed by its fields
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
    #   # ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    #   # │ bar    ┆ 2   ┆ b   ┆ null ┆ [3]       ┆ womp  │
    #   # └────────┴─────┴─────┴──────┴───────────┴───────┘
    def unnest(names)
      if names.is_a?(String)
        names = [names]
      end
      _from_rbdf(_df.unnest(names))
    end

    private

    def initialize_copy(other)
      super
      self._df = _df._clone
    end

    def hash_to_rbdf(data, columns: nil)
      if !columns.nil?
        columns, dtypes = _unpack_columns(columns, lookup_names: data.keys)

        if data.empty? && dtypes
          data_series = columns.map { |name| Series.new(name, [], dtype: dtypes[name])._s }
        else
          data_series = data.map { |name, values| Series.new(name, values, dtype: dtypes[name])._s }
        end
        data_series = _handle_columns_arg(data_series, columns: columns)
        return RbDataFrame.new(data_series)
      end

      RbDataFrame.read_hash(data)
    end

    def _unpack_columns(columns, lookup_names: nil)
      [columns.keys, columns]
    end

    def _handle_columns_arg(data, columns: nil)
      if columns.nil?
        data
      else
        if data.empty?
          columns.map { |c| Series.new(c, nil)._s }
        elsif data.length == columns.length
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

    def sequence_to_rbdf(data, columns: nil, orient: nil)
      if columns || orient
        raise Todo
      end
      RbDataFrame.new(data.map(&:_s))
    end

    def series_to_rbdf(data, columns: nil)
      if columns
        raise Todo
      end
      RbDataFrame.new([data._s])
    end

    def wrap_ldf(ldf)
      LazyFrame._from_rbldf(ldf)
    end

    def _from_rbdf(rb_df)
      self.class._from_rbdf(rb_df)
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
        raise ArgmentError, "DataFrame columns do not match"
      end
      if shape != other.shape
        raise ArgmentError, "DataFrame dimensions do not match"
      end

      suffix = "__POLARS_CMP_OTHER"
      other_renamed = other.select(Polars.all.suffix(suffix))
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
        if other.is_a?(Array)
          raise ArgumentError, "Operation not supported."
        end

        other = Series.new("", [other])
      end
      other
    end
  end
end

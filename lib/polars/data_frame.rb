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
    def self._read_parquet(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_parquet(file))
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6, 7, 8],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6, 7, 8],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6.0, 7.0, 8.0],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6.0, 7.0, 8.0],
    #     "ham" => ["a", "b", "c"]
    #   })
    #   df.schema
    #   # => {"foo"=>:i64, "bar"=>:f64, "ham"=>:str}
    def schema
      columns.zip(dtypes).to_h
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

    # def *(other)
    # end

    # def /(other)
    # end

    # def +(other)
    # end

    # def -(other)
    # end

    # def %(other)
    # end

    #
    def to_s
      _df.to_s
    end
    alias_method :inspect, :to_s

    def include?(name)
      columns.include?(name)
    end

    # def each
    # end

    # def _pos_idx
    # end

    # def _pos_idxs
    # end

    #
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

    #
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6, 7, 8],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3, 4, 5],
    #     "bar" => [6, 7, 8, 9, 10],
    #     "ham" => ["a", "b", "c", "d", "e"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "key" => ["a", "b", "c"],
    #     "val" => [1, 2, 3]
    #   })
    #   df.reverse()
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6, 7, 8],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "a" => [1, 2, 3, 4],
    #     "b" => [0.5, 4, 10, 13],
    #     "c" => [true, true, false, true]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6, 7, 8],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "a" => [1.0, 2.8, 3.0],
    #     "b" => [4, 5, nil],
    #     "c" => [true, false, true],
    #     "d" => [nil, "b", "c"],
    #     "e" => ["usd", "eur", nil]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6, 7, 8],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6.0, 7.0, 8.0],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df1 = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6.0, 7.0, 8.0],
    #     "ham" => ["a", "b", "c"]
    #   })
    #   df2 = Polars::DataFrame.new({
    #     "foo" => [3, 2, 1],
    #     "bar" => [8.0, 7.0, 6.0],
    #     "ham" => ["c", "b", "a"]
    #   })
    #   df1.frame_equal(df1)
    #   # => true
    #   df1.frame_equal(df2)
    #   # => false
    def frame_equal(other, null_equal: true)
      _df.frame_equal(other._df, null_equal)
    end

    # def replace
    # end

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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3],
    #     "bar" => [6.0, 7.0, 8.0],
    #     "ham" => ["a", "b", "c"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3, 4, 5],
    #     "bar" => [6, 7, 8, 9, 10],
    #     "ham" => ["a", "b", "c", "d", "e"]
    #   })
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
    #   df = Polars::DataFrame.new({
    #     "foo" => [1, 2, 3, 4, 5],
    #     "bar" => [6, 7, 8, 9, 10],
    #     "ham" => ["a", "b", "c", "d", "e"]
    #   })
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

    # def drop_nulls
    # end

    # def pipe
    # end

    # def with_row_count
    # end

    #
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

    #
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

    #
    def with_column(column)
      lazy
        .with_column(column)
        .collect(no_optimization: true, string_cache: false)
    end

    # def hstack
    # end

    # def vstack
    # end

    #
    def extend(other)
      _df.extend(other._df)
      self
    end

    # def drop
    # end

    # def drop_in_place
    # end

    # def cleared
    # end

    # clone handled by initialize_copy

    #
    def get_columns
      _df.get_columns.map { |s| Utils.wrap_s(s) }
    end

    def get_column(name)
      self[name]
    end

    # def fill_null
    # end

    #
    def fill_nan(fill_value)
      lazy.fill_nan(fill_value).collect(no_optimization: true)
    end

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

    # def shift
    # end

    # def shift_and_fill
    # end

    #
    def is_duplicated
      Utils.wrap_s(_df.is_duplicated)
    end

    def is_unique
      Utils.wrap_s(_df.is_unique)
    end

    def lazy
      wrap_ldf(_df.lazy)
    end

    def select(exprs)
      _from_rbdf(
        lazy
          .select(exprs)
          .collect(no_optimization: true, string_cache: false)
          ._df
      )
    end

    def with_columns(exprs)
      if !exprs.nil? && !exprs.is_a?(Array)
        exprs = [exprs]
      end
      lazy
        .with_columns(exprs)
        .collect(no_optimization: true, string_cache: false)
    end

    def n_chunks(strategy: "first")
      if strategy == "first"
        _df.n_chunks
      elsif strategy == "all"
        get_columns.map(&:n_chunks)
      else
        raise ArgumentError, "Strategy: '{strategy}' not understood. Choose one of {{'first',  'all'}}"
      end
    end

    def max(axis: 0)
      if axis == 0
        _from_rbdf(_df.max)
      elsif axis == 1
        Utils.wrap_s(_df.hmax)
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

    def min(axis: 0)
      if axis == 0
        _from_rbdf(_df.min)
      elsif axis == 1
        Utils.wrap_s(_df.hmin)
      else
        raise ArgumentError, "Axis should be 0 or 1."
      end
    end

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

    def std(ddof: 1)
      _from_rbdf(_df.std(ddof))
    end

    def var(ddof: 1)
      _from_rbdf(_df.var(ddof))
    end

    def median
      _from_rbdf(_df.median)
    end

    # def product
    # end

    # def quantile(quantile, interpolation: "nearest")
    # end

    # def to_dummies
    # end

    # def unique
    # end

    # def n_unique
    # end

    #
    def rechunk
      _from_rbdf(_df.rechunk)
    end

    def null_count
      _from_rbdf(_df.null_count)
    end

    # def sample
    # end

    # def fold
    # end

    # def row
    # end

    # def rows
    # end

    # def shrink_to_fit
    # end

    # def take_every
    # end

    # def hash_rows
    # end

    # def interpolate
    # end

    #
    def is_empty
      height == 0
    end
    alias_method :empty?, :is_empty

    # def to_struct(name)
    # end

    # def unnest
    # end

    private

    def initialize_copy(other)
      super
      self._df = _df._clone
    end

    def hash_to_rbdf(data, columns: nil)
      if !columns.nil?
        columns, dtypes = _unpack_columns(columns, lookup_names: data.keys)

        if !data && dtypes
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
        if !data
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
  end
end

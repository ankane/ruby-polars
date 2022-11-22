module Polars
  class DataFrame
    attr_accessor :_df

    def initialize(data = nil)
      if defined?(ActiveRecord) && (data.is_a?(ActiveRecord::Relation) || data.is_a?(ActiveRecord::Result))
        result = data.is_a?(ActiveRecord::Result) ? data : data.connection.select_all(data.to_sql)
        data = {}
        result.columns.each_with_index do |k, i|
          data[k] = result.rows.map { |r| r[i] }
        end
      end

      if data.nil?
        self._df = hash_to_rbdf({})
      elsif data.is_a?(Hash)
        data = data.transform_keys { |v| v.is_a?(Symbol) ? v.to_s : v }
        self._df = hash_to_rbdf(data)
      elsif data.is_a?(Array)
        self._df = sequence_to_rbdf(data)
      elsif data.is_a?(Series)
        self._df = series_to_rbdf(data)
      else
        raise ArgumentError, "DataFrame constructor called with unsupported type; got #{data.class.name}"
      end
    end

    def self._from_rbdf(rb_df)
      df = DataFrame.allocate
      df._df = rb_df
      df
    end

    def self._read_csv(file, has_header: true)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_csv(file, has_header))
    end

    def self._read_parquet(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_parquet(file))
    end

    def self._read_json(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_json(file))
    end

    def self._read_ndjson(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _from_rbdf(RbDataFrame.read_ndjson(file))
    end

    def shape
      _df.shape
    end

    def height
      _df.height
    end

    def width
      _df.width
    end

    def columns
      _df.columns
    end

    def dtypes
      _df.dtypes.map(&:to_sym)
    end

    def to_s
      _df.to_s
    end
    alias_method :inspect, :to_s

    def include?(name)
      columns.include?(name)
    end

    def [](name)
      Utils.wrap_s(_df.column(name))
    end

    def to_series(index = 0)
      if index < 0
        index = columns.length + index
      end
      Utils.wrap_s(_df.select_at_idx(index))
    end

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

    def write_ndjson(file)
      if file.is_a?(String) || (defined?(Pathname) && file.is_a?(Pathname))
        file = Utils.format_path(file)
      end

      _df.write_ndjson(file)
      nil
    end

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
        buffer.rewind
        return buffer.read.force_encoding(Encoding::UTF_8)
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

    def reverse
      select(Polars.col("*").reverse)
    end

    def rename(mapping)
      lazy.rename(mapping).collect(no_optimization: true)
    end

    def filter(predicate)
      lazy.filter(predicate).collect
    end

    def sort(by, reverse: false, nulls_last: false)
      _from_rbdf(_df.sort(by, reverse, nulls_last))
    end

    def frame_equal(other, null_equal: true)
      _df.frame_equal(other._df, null_equal)
    end

    def slice(offset, length = nil)
      if !length.nil? && length < 0
        length = height - offset + length
      end
      _from_rbdf(_df.slice(offset, length))
    end

    def limit(n = 5)
      head(n)
    end

    def head(n = 5)
      _from_rbdf(_df.head(n))
    end

    def tail(n = 5)
      _from_rbdf(_df.tail(n))
    end

    def groupby(by, maintain_order: false)
      lazy.groupby(by, maintain_order: maintain_order)
    end

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

    def with_column(column)
      lazy
        .with_column(column)
        .collect(no_optimization: true, string_cache: false)
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

    def with_columns(exprs)
      if !exprs.nil? && !exprs.is_a?(Array)
        exprs = [exprs]
      end
      lazy
        .with_columns(exprs)
        .collect(no_optimization: true, string_cache: false)
    end

    def rechunk
      _from_rbdf(_df.rechunk)
    end

    def null_count
      _from_rbdf(_df.null_count)
    end

    private

    def initialize_copy(other)
      super
      self._df = _df._clone
    end

    def hash_to_rbdf(data)
      RbDataFrame.read_hash(data)
    end

    def sequence_to_rbdf(data)
      RbDataFrame.new(data.map(&:_s))
    end

    def series_to_rbdf(data)
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

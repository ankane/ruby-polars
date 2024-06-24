module Polars
  # @private
  module Utils
    DTYPE_TEMPORAL_UNITS = ["ns", "us", "ms"]

    def self.col(name)
      Polars.col(name)
    end

    def self.arrlen(obj)
      if obj.is_a?(Range)
        # size only works for numeric ranges
        obj.to_a.length
      elsif obj.is_a?(::String)
        nil
      else
        obj.length
      end
    rescue
      nil
    end

    def self._to_ruby_time(value)
      if value == 0
        ::Time.utc(2000, 1, 1)
      else
        seconds, nanoseconds = value.divmod(1_000_000_000)
        minutes, seconds = seconds.divmod(60)
        hours, minutes = minutes.divmod(60)
        ::Time.utc(2000, 1, 1, hours, minutes, seconds, nanoseconds / 1000.0)
      end
    end

    def self._to_ruby_duration(value, time_unit = "ns")
      if time_unit == "ns"
        value / 1e9
      elsif time_unit == "us"
        value / 1e6
      elsif time_unit == "ms"
        value / 1e3
      else
        raise ArgumentError, "time_unit must be one of {{'ns', 'us', 'ms'}}, got #{time_unit}"
      end
    end

    def self._to_ruby_date(value)
      # days to seconds
      # important to create from utc. Not doing this leads
      # to inconsistencies dependent on the timezone you are in.
      ::Time.at(value * 86400).utc.to_date
    end

    def self._to_ruby_datetime(value, time_unit = "ns", time_zone = nil)
      if time_zone.nil? || time_zone == ""
        if time_unit == "ns"
          ::Time.at(value / 1000000000, value % 1000000000, :nsec).utc
        elsif time_unit == "us"
          ::Time.at(value / 1000000, value % 1000000, :usec).utc
        elsif time_unit == "ms"
          ::Time.at(value / 1000, value % 1000, :millisecond).utc
        else
          raise ArgumentError, "time_unit must be one of {{'ns', 'us', 'ms'}}, got #{time_unit}"
        end
      else
        raise Todo
      end
    end

    def self._to_ruby_decimal(digits, scale)
      BigDecimal("#{digits}e#{scale}")
    end

    def self.selection_to_rbexpr_list(exprs)
      if exprs.is_a?(::String) || exprs.is_a?(Symbol) || exprs.is_a?(Expr) || exprs.is_a?(Series)
        exprs = [exprs]
      end

      exprs.map { |e| expr_to_lit_or_expr(e, str_to_lit: false)._rbexpr }
    end

    def self.expr_to_lit_or_expr(expr, str_to_lit: true)
      if (expr.is_a?(::String) || expr.is_a?(Symbol)) && !str_to_lit
        col(expr)
      elsif expr.is_a?(Integer) || expr.is_a?(Float) || expr.is_a?(::String) || expr.is_a?(Symbol) || expr.is_a?(Series) || expr.nil?
        lit(expr)
      elsif expr.is_a?(Expr)
        expr
      else
        raise ArgumentError, "did not expect value #{expr} of type #{expr.class.name}, maybe disambiguate with Polars.lit or Polars.col"
      end
    end

    def self.lit(value)
      Polars.lit(value)
    end

    def self.normalize_filepath(path, check_not_directory: true)
      path = File.expand_path(path)
      if check_not_directory && File.exist?(path) && Dir.exist?(path)
        raise ArgumentError, "Expected a file path; #{path} is a directory"
      end
      path
    end

    # TODO fix
    def self.is_polars_dtype(data_type, include_unknown: false)
      if data_type == Unknown
        return include_unknown
      end
      data_type.is_a?(Symbol) || data_type.is_a?(::String) || data_type.is_a?(DataType) || (data_type.is_a?(Class) && data_type < DataType)
    end

    def self.map_rb_type_to_dtype(ruby_dtype)
      if ruby_dtype == Float
        Float64
      elsif ruby_dtype == Integer
        Int64
      elsif ruby_dtype == ::String
        Utf8
      elsif ruby_dtype == TrueClass || ruby_dtype == FalseClass
        Boolean
      elsif ruby_dtype == DateTime || ruby_dtype == ::Time || (defined?(ActiveSupport::TimeWithZone) && ruby_dtype == ActiveSupport::TimeWithZone)
        Datetime.new("ns")
      elsif ruby_dtype == ::Date
        Date
      elsif ruby_dtype == ::Array
        List
      elsif ruby_dtype == NilClass
        Null
      else
        raise TypeError, "Invalid type"
      end
    end

    # TODO fix
    def self.rb_type_to_dtype(data_type)
      if is_polars_dtype(data_type)
        data_type = data_type.to_s if data_type.is_a?(Symbol)
        return data_type
      end

      begin
        map_rb_type_to_dtype(data_type)
      rescue TypeError
        raise ArgumentError, "Conversion of Ruby data type #{data_type.inspect} to Polars data type not implemented."
      end
    end

    def self._process_null_values(null_values)
      if null_values.is_a?(Hash)
        null_values.to_a
      else
        null_values
      end
    end

    def self._prepare_row_count_args(row_count_name = nil, row_count_offset = 0)
      if !row_count_name.nil?
        [row_count_name, row_count_offset]
      else
        nil
      end
    end

    def self.handle_projection_columns(columns)
      projection = nil
      if columns
        raise Todo
        # if columns.is_a?(::String) || columns.is_a?(Symbol)
        #   columns = [columns]
        # elsif is_int_sequence(columns)
        #   projection = columns.to_a
        #   columns = nil
        # elsif !is_str_sequence(columns)
        #   raise ArgumentError, "columns arg should contain a list of all integers or all strings values."
        # end
      end
      [projection, columns]
    end

    def self.scale_bytes(sz, to:)
      scaling_factor = {
        "b" => 1,
        "k" => 1024,
        "m" => 1024 ** 2,
        "g" => 1024 ** 3,
        "t" => 1024 ** 4
      }[to[0]]
      if scaling_factor > 1
        sz / scaling_factor.to_f
      else
        sz
      end
    end

    def self.bool?(value)
      value.is_a?(TrueClass) || value.is_a?(FalseClass)
    end

    def self.strlike?(value)
      value.is_a?(::String) || value.is_a?(Symbol)
    end

    def self.pathlike?(value)
      value.is_a?(::String) || (defined?(Pathname) && value.is_a?(Pathname))
    end

    def self._is_iterable_of(val, eltype)
      val.all? { |x| x.is_a?(eltype) }
    end

    def self.is_bool_sequence(val)
      val.is_a?(::Array) && val.all? { |x| x == true || x == false }
    end

    def self.is_dtype_sequence(val)
      val.is_a?(::Array) && val.all? { |x| is_polars_dtype(x) }
    end

    def self.is_int_sequence(val)
      val.is_a?(::Array) && _is_iterable_of(val, Integer)
    end

    def self.is_expr_sequence(val)
      val.is_a?(::Array) && _is_iterable_of(val, Expr)
    end

    def self.is_rbexpr_sequence(val)
      val.is_a?(::Array) && _is_iterable_of(val, RbExpr)
    end

    def self.is_str_sequence(val, allow_str: false)
      if allow_str == false && val.is_a?(::String)
        false
      else
        val.is_a?(::Array) && _is_iterable_of(val, ::String)
      end
    end

    def self.local_file?(file)
      Dir.glob(file).any?
    end

    def self._check_arg_is_1byte(arg_name, arg, can_be_empty = false)
      if arg.is_a?(::String)
        arg_byte_length = arg.bytesize
        if can_be_empty
          if arg_byte_length > 1
            raise ArgumentError, "#{arg_name} should be a single byte character or empty, but is #{arg_byte_length} bytes long."
          end
        elsif arg_byte_length != 1
          raise ArgumentError, "#{arg_name} should be a single byte character, but is #{arg_byte_length} bytes long."
        end
      end
    end

    def self._expand_selectors(frame, *items)
      items_iter = _parse_inputs_as_iterable(items)

      expanded = []
      items_iter.each do |item|
        if is_selector(item)
          selector_cols = expand_selector(frame, item)
          expanded.concat(selector_cols)
        else
          expanded << item
        end
      end
      expanded
    end

    # TODO
    def self.is_selector(obj)
      false
    end

    def self.parse_interval_argument(interval)
      if interval.include?(" ")
        interval = interval.gsub(" ", "")
      end
      interval.downcase
    end

    def self.validate_rolling_by_aggs_arguments(weights, center:)
      if !weights.nil?
        msg = "`weights` is not supported in `rolling_*(..., by=...)` expression"
        raise InvalidOperationError, msg
      end
      if center
        msg = "`center=True` is not supported in `rolling_*(..., by=...)` expression"
        raise InvalidOperationError, msg
      end
    end

    def self.validate_rolling_aggs_arguments(window_size, closed)
      if window_size.is_a?(::String)
        begin
          window_size = window_size.delete_suffix("i").to_i
        rescue
          msg = "Expected a string of the form 'ni', where `n` is a positive integer, got: #{window_size}"
          raise InvalidOperationError, msg
        end
      end
      if !closed.nil?
        msg = "`closed` is not supported in `rolling_*(...)` expression"
        raise InvalidOperationError, msg
      end
      window_size
    end

    def self.extend_bool(value, n_match, value_name, match_name)
      values = bool?(value) ? [value] * n_match : value
      if n_match != values.length
        msg = "the length of `#{value_name}` (#{values.length}) does not match the length of `#{match_name}` (#{n_match})"
        raise ValueError, msg
      end
      values
    end
  end
end

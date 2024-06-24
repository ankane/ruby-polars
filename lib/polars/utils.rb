module Polars
  # @private
  module Utils
    DTYPE_TEMPORAL_UNITS = ["ns", "us", "ms"]

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

    def self.bool?(value)
      value.is_a?(TrueClass) || value.is_a?(FalseClass)
    end

    def self.strlike?(value)
      value.is_a?(::String) || value.is_a?(Symbol)
    end

    def self.pathlike?(value)
      value.is_a?(::String) || (defined?(Pathname) && value.is_a?(Pathname))
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
  end
end

module Polars
  # @private
  module Utils
    DTYPE_TEMPORAL_UNITS = ["ns", "us", "ms"]

    def self.is_polars_dtype(dtype, include_unknown: false)
      is_dtype = dtype.is_a?(DataType) || (dtype.is_a?(Class) && dtype < DataType)

      if !include_unknown
        is_dtype && dtype != Unknown
      else
        is_dtype
      end
    end

    def self.is_column(obj)
      obj.is_a?(Expr) && obj.meta.is_column
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
        return data_type
      end

      begin
        map_rb_type_to_dtype(data_type)
      rescue TypeError
        raise ArgumentError, "Conversion of Ruby data type #{data_type.inspect} to Polars data type not implemented."
      end
    end

    def self.parse_row_index_args(row_index_name = nil, row_index_offset = 0)
      if row_index_name.nil?
        nil
      else
        [row_index_name, row_index_offset]
      end
    end

    def self.handle_projection_columns(columns)
      projection = nil
      if columns
        if columns.is_a?(::String)
          columns = [columns]
        elsif is_int_sequence(columns)
          projection = columns.to_a
          columns = nil
        elsif !is_str_sequence(columns)
          raise ArgumentError, "columns arg should contain a list of all integers or all strings values."
        end
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

    def self.is_selector(obj)
      obj.is_a?(Selector)
    end

    def self.expand_selector(target, selector, strict: true)
      if target.is_a?(Hash)
        target = DataFrame.new(schema: target)
      end

      if !is_selector(selector) && !is_polars_dtype(selector)
        msg = "expected a selector; found #{selector.inspect} instead."
        raise TypeError, msg
      end

      if is_selector(selector)
        target.select(selector).columns
      else
        target.select(Polars.col(selector)).columns
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

    def self.parse_interval_argument(interval)
      if interval.include?(" ")
        interval = interval.gsub(" ", "")
      end
      interval.downcase
    end

    def self.parse_into_dtype(input)
      if is_polars_dtype(input)
        input
      else
        parse_rb_type_into_dtype(input)
      end
    end

    def self.try_parse_into_dtype(input)
      parse_into_dtype(input)
    rescue TypeError
      nil
    end

    def self.parse_rb_type_into_dtype(input)
      if input == Integer
        Int64.new
      elsif input == Float
        Float64.new
      elsif input == ::String
        String.new
      elsif input == ::Time || input == ::DateTime || (defined?(ActiveSupport::TimeWithZone) && input == ActiveSupport::TimeWithZone)
        Datetime.new("ns")
      elsif input == ::Date
        Date.new
      elsif input.nil?
        Null.new
      elsif input == ::Array
        List
      # this is required as pass through. Don't remove
      elsif input == Unknown
        Unknown
      else
        _raise_on_invalid_dtype(input)
      end
    end

    def self._raise_on_invalid_dtype(input)
      # TODO improve
      input_type = input.inspect
      msg = "cannot parse input #{input_type} into Polars data type"
      raise TypeError, msg
    end

    def self.re_escape(s)
      # escapes _only_ those metachars with meaning to the rust regex crate
      Plr.re_escape(s)
    end

    def self.parse_into_datatype_expr(input)
      if input.is_a?(DataTypeExpr)
        input
      else
        parse_into_dtype(input).to_dtype_expr
      end
    end
  end
end

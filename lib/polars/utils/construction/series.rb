module Polars
  module Utils
    def self.sequence_to_rbseries(name, values, dtype: nil, strict: true, nan_to_null: false)
      ruby_dtype = nil

      if values.is_a?(Range)
        if values.begin.is_a?(::String)
          values = values.to_a
        else
          return range_to_series(name, values, dtype: dtype)._s
        end
      end

      if values.length == 0 && dtype.nil?
        dtype = Null
      elsif [List, Array].include?(dtype)
        ruby_dtype = ::Array
      end

      rb_temporal_types = [::Date, ::DateTime, ::Time]
      rb_temporal_types << ActiveSupport::TimeWithZone if defined?(ActiveSupport::TimeWithZone)

      value = get_first_non_none(values)
      if !value.nil?
        if value.is_a?(Hash) && dtype != Object
          return DataFrame.new(values).to_struct(name)._s
        end
      end

      if !dtype.nil? && is_polars_dtype(dtype) && !dtype.nested? && dtype != Unknown && ruby_dtype.nil?
        constructor = polars_type_to_constructor(dtype)
        rbseries = _construct_series_with_fallbacks(
          constructor, name, values, dtype, strict: strict
        )

        if [Date, Datetime, Duration, Time, Boolean, Categorical, Enum].include?(dtype) || dtype.is_a?(Decimal) || dtype.is_a?(Categorical)
          if rbseries.dtype != dtype
            rbseries = rbseries.cast(dtype, true, false)
          end
        end

        # Uninstanced Decimal is a bit special and has various inference paths
        if dtype == Decimal
          if rbseries.dtype == String
            rbseries = rbseries.str_to_decimal_infer(0)
          elsif rbseries.dtype.float?
            # Go through string so we infer an appropriate scale.
            rbseries = rbseries.cast(
              String, strict, false
            ).str_to_decimal_infer(0)
          elsif rbseries.dtype.integer? || rbseries.dtype == Null
            rbseries = rbseries.cast(
              Decimal.new(nil, 0), strict, false
            )
          elsif !rbseries.dtype.is_a?(Decimal)
            msg = "can't convert #{rbseries.dtype} to Decimal"
            raise TypeError, msg
          end
        end

        return rbseries

      elsif dtype == Struct
        struct_schema = dtype.is_a?(Struct) ? dtype.to_schema : nil
        empty = {}

        data = []
        invalid = []
        values.each_with_index do |v, i|
          if v.nil?
            invalid << i
            data << empty
          else
            data << v
          end
        end

        return sequence_to_rbdf(
          values.map { |v| v.nil? ? empty : v },
          schema: struct_schema,
          orient: "row",
        ).to_struct(name, invalid)
      end

      if ruby_dtype.nil?
        if value.nil?
          constructor = polars_type_to_constructor(Null)
          return constructor.(name, values, strict)
        end

        ruby_dtype = value.class
      end

      # temporal branch
      if rb_temporal_types.include?(ruby_dtype)
        if dtype.nil?
          dtype = parse_into_dtype(ruby_dtype)
        elsif rb_temporal_types.include?(dtype)
          dtype = parse_into_dtype(dtype)
        end

        values_dtype = value.nil? ? nil : try_parse_into_dtype(value.class)
        if !values_dtype.nil? && values_dtype.float?
          msg = "'float' object cannot be interpreted as a #{ruby_dtype.name.inspect}"
          raise TypeError, msg
        end

        rb_series = RbSeries.new_from_any_values(name, values, strict)

        time_unit = dtype.respond_to?(:time_unit) ? dtype.time_unit : nil
        time_zone = dtype.respond_to?(:time_zone) ? dtype.time_zone : nil

        if dtype.temporal? && values_dtype == String && dtype != Duration
          s = wrap_s(rb_series).str.strptime(dtype, strict: strict)
        elsif !time_unit.nil? && values_dtype != Date
          s = wrap_s(rb_series).dt.cast_time_unit(time_unit)
        else
          s = wrap_s(rb_series)
        end

        if dtype == Datetime && !time_zone.nil?
          return s.dt.convert_time_zone(time_zone)._s
        end
        s._s

      elsif defined?(Numo::NArray) && value.is_a?(Numo::NArray) && value.shape.length == 1
        raise Todo

      elsif ruby_dtype == ::Array
        if dtype.nil?
          RbSeries.new_from_any_values(name, values, strict)
        elsif dtype.is_a?(Object)
          RbSeries.new_object(name, values, strict)
        else
          inner_dtype = dtype.respond_to?(:inner) ? dtype.inner : nil
          if !inner_dtype.nil?
            rbseries_list =
              values.map do |value|
                if value.nil?
                  nil
                else
                  sequence_to_rbseries(
                    "",
                    value,
                    dtype: inner_dtype,
                    strict: strict,
                    nan_to_null: nan_to_null,
                  )
                end
              end
            rbseries = RbSeries.new_series_list(name, rbseries_list, strict)
          else
            # panics in Python
            raise Todo if dtype.eql?(Array)

            rbseries = RbSeries.new_from_any_values_and_dtype(
              name, values, dtype, strict
            )
          end
          if dtype != rbseries.dtype
            rbseries = rbseries.cast(dtype, false, false)
          end
          rbseries
        end

      elsif ruby_dtype == Series
        RbSeries.new_series_list(
          name, values.map { |v| !v.nil? ? v._s : v }, strict
        )

      elsif ruby_dtype == RbSeries
        RbSeries.new_series_list(name, values, strict)
      else
        constructor =
          if value.is_a?(::String)
            if value.encoding == Encoding::UTF_8
              RbSeries.method(:new_str)
            else
              RbSeries.method(:new_binary)
            end
          elsif value.is_a?(Integer) && values.any? { |v| v.is_a?(Float) }
            # TODO improve performance
            RbSeries.method(:new_opt_f64)
          else
            rb_type_to_constructor(value.class)
          end

        _construct_series_with_fallbacks(constructor, name, values, dtype, strict: strict)
      end
    end

    def self._construct_series_with_fallbacks(constructor, name, values, dtype, strict:)
      begin
        constructor.(name, values, strict)
      rescue
        if dtype.nil?
          RbSeries.new_from_any_values(name, values, strict)
        else
          RbSeries.new_from_any_values_and_dtype(name, values, dtype, strict)
        end
      end
    end

    def self.numo_to_rbseries(name, values, strict: true, nan_to_null: false)
      # not needed yet
      # if !values.contiguous?
      # end

      if values.shape.length == 1
        values, dtype = numo_values_and_dtype(values)
        constructor = numo_type_to_constructor(dtype)
        constructor.(
          name, values.to_a, [Numo::SFloat, Numo::DFloat].include?(dtype) ? nan_to_null : strict
        )
      else
        original_shape = values.shape
        values_1d = values.reshape(original_shape.inject(&:*))

        rb_s = numo_to_rbseries(
          name,
          values_1d,
          strict: strict,
          nan_to_null: nan_to_null
        )
        Utils.wrap_s(rb_s).reshape(original_shape)._s
      end
    end

    def self.series_to_rbseries(name, values, dtype: nil, strict: true)
      s = values.clone
      if !dtype.nil? && dtype != s.dtype
        s = s.cast(dtype, strict: strict)
      end
      if !name.nil?
        s = s.alias(name)
      end
      s._s
    end

    def self.dataframe_to_rbseries(
      name,
      values,
      dtype: nil,
      strict: true
    )
      if values.width > 1
        name ||= ""
        s = values.to_struct(name)
      elsif values.width == 1
        s = values.to_series
        if !name.nil?
          s = s.alias(name)
        end
      else
        msg = "cannot initialize Series from DataFrame without any columns"
        raise TypeError, msg
      end

      if !dtype.nil? && dtype != s.dtype
        s = s.cast(dtype, strict: strict)
      end

      s._s
    end

    # TODO move rest

    POLARS_TYPE_TO_CONSTRUCTOR = {
      Float16 => RbSeries.method(:new_opt_f16),
      Float32 => RbSeries.method(:new_opt_f32),
      Float64 => RbSeries.method(:new_opt_f64),
      Int8 => RbSeries.method(:new_opt_i8),
      Int16 => RbSeries.method(:new_opt_i16),
      Int32 => RbSeries.method(:new_opt_i32),
      Int64 => RbSeries.method(:new_opt_i64),
      Int128 => RbSeries.method(:new_opt_i128),
      UInt8 => RbSeries.method(:new_opt_u8),
      UInt16 => RbSeries.method(:new_opt_u16),
      UInt32 => RbSeries.method(:new_opt_u32),
      UInt64 => RbSeries.method(:new_opt_u64),
      UInt128 => RbSeries.method(:new_opt_u128),
      Decimal => RbSeries.method(:new_decimal),
      Date => RbSeries.method(:new_from_any_values),
      Datetime => RbSeries.method(:new_from_any_values),
      Duration => RbSeries.method(:new_from_any_values),
      Time => RbSeries.method(:new_from_any_values),
      Boolean => RbSeries.method(:new_opt_bool),
      Utf8 => RbSeries.method(:new_str),
      Object => RbSeries.method(:new_object),
      Categorical => RbSeries.method(:new_str),
      Enum => RbSeries.method(:new_str),
      Binary => RbSeries.method(:new_binary),
      Null => RbSeries.method(:new_null)
    }

    SYM_TYPE_TO_CONSTRUCTOR = {
      f32: RbSeries.method(:new_opt_f32),
      f64: RbSeries.method(:new_opt_f64),
      i8: RbSeries.method(:new_opt_i8),
      i16: RbSeries.method(:new_opt_i16),
      i32: RbSeries.method(:new_opt_i32),
      i64: RbSeries.method(:new_opt_i64),
      i128: RbSeries.method(:new_opt_i128),
      u8: RbSeries.method(:new_opt_u8),
      u16: RbSeries.method(:new_opt_u16),
      u32: RbSeries.method(:new_opt_u32),
      u64: RbSeries.method(:new_opt_u64),
      u128: RbSeries.method(:new_opt_u128),
      bool: RbSeries.method(:new_opt_bool),
      str: RbSeries.method(:new_str)
    }

    def self.polars_type_to_constructor(dtype)
      if dtype.is_a?(Array)
        lambda do |name, values, strict|
          RbSeries.new_array(dtype.width, dtype.inner, name, values, strict)
        end
      elsif dtype.is_a?(Class) && dtype < DataType
        POLARS_TYPE_TO_CONSTRUCTOR.fetch(dtype)
      elsif dtype.is_a?(DataType)
        POLARS_TYPE_TO_CONSTRUCTOR.fetch(dtype.class)
      else
        SYM_TYPE_TO_CONSTRUCTOR.fetch(dtype.to_sym)
      end
    rescue KeyError
      raise ArgumentError, "Cannot construct RbSeries for type #{dtype}."
    end

    RB_TYPE_TO_CONSTRUCTOR = {
      Float => RbSeries.method(:new_opt_f64),
      Integer => RbSeries.method(:new_opt_i64),
      TrueClass => RbSeries.method(:new_opt_bool),
      FalseClass => RbSeries.method(:new_opt_bool),
      BigDecimal => RbSeries.method(:new_decimal),
      NilClass => RbSeries.method(:new_null)
    }

    def self.rb_type_to_constructor(dtype)
      RB_TYPE_TO_CONSTRUCTOR.fetch(dtype)
    rescue KeyError
      RbSeries.method(:new_object)
    end

    def self.numo_values_and_dtype(values)
      [values, values.class]
    end

    def self.numo_type_to_constructor(dtype)
      {
        Numo::Float32 => RbSeries.method(:new_opt_f32),
        Numo::Float64 => RbSeries.method(:new_opt_f64),
        Numo::Int8 => RbSeries.method(:new_opt_i8),
        Numo::Int16 => RbSeries.method(:new_opt_i16),
        Numo::Int32 => RbSeries.method(:new_opt_i32),
        Numo::Int64 => RbSeries.method(:new_opt_i64),
        Numo::UInt8 => RbSeries.method(:new_opt_u8),
        Numo::UInt16 => RbSeries.method(:new_opt_u16),
        Numo::UInt32 => RbSeries.method(:new_opt_u32),
        Numo::UInt64 => RbSeries.method(:new_opt_u64)
      }.fetch(dtype)
    rescue KeyError
      RbSeries.method(:new_object)
    end
  end
end

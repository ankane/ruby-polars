module Polars
  # Base class for all Polars data types.
  class DataType
    def self.base_type
      self
    end

    def base_type
      is_a?(DataType) ? self.class : self
    end

    def self.nested?
      false
    end

    def nested?
      self.class.nested?
    end
  end

  # Base class for numeric data types.
  class NumericType < DataType
  end

  # Base class for integral data types.
  class IntegralType < NumericType
  end

  # Base class for fractional data types.
  class FractionalType < NumericType
  end

  # Base class for float data types.
  class FloatType < FractionalType
  end

  # Base class for temporal data types.
  class TemporalType < DataType
  end

  # Base class for nested data types.
  class NestedType < DataType
    def self.nested?
      true
    end
  end

  # 8-bit signed integer type.
  class Int8 < IntegralType
  end

  # 16-bit signed integer type.
  class Int16 < IntegralType
  end

  # 32-bit signed integer type.
  class Int32 < IntegralType
  end

  # 64-bit signed integer type.
  class Int64 < IntegralType
  end

  # 8-bit unsigned integer type.
  class UInt8 < IntegralType
  end

  # 16-bit unsigned integer type.
  class UInt16 < IntegralType
  end

  # 32-bit unsigned integer type.
  class UInt32 < IntegralType
  end

  # 64-bit unsigned integer type.
  class UInt64 < IntegralType
  end

  # 32-bit floating point type.
  class Float32 < FloatType
  end

  # 64-bit floating point type.
  class Float64 < FloatType
  end

  # Decimal 128-bit type with an optional precision and non-negative scale.
  #
  # NOTE: this is an experimental work-in-progress feature and may not work as expected.
  class Decimal < FractionalType
    attr_reader :precision, :scale

    def initialize(precision, scale)
      @precision = precision
      @scale = scale
    end

    def to_s
      "#{self.class.name}(precision: #{precision.inspect}, scale: #{scale.inspect})"
    end
  end

  # Boolean type.
  class Boolean < DataType
  end

  # UTF-8 encoded string type.
  class Utf8 < DataType
  end

  # Binary type.
  class Binary < DataType
  end

  # Calendar date type.
  class Date < TemporalType
  end

  # Time of day type.
  class Time < TemporalType
  end

  # Calendar date and time type.
  class Datetime < TemporalType
    attr_reader :time_unit, :time_zone
    alias_method :tu, :time_unit

    def initialize(time_unit = "us", time_zone = nil)
      @time_unit = time_unit || "us"
      @time_zone = time_zone
    end

    def ==(other)
      if other.eql?(Datetime)
        true
      elsif other.is_a?(Datetime)
        time_unit == other.time_unit && time_zone == other.time_zone
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(time_unit: #{time_unit.inspect}, time_zone: #{time_zone.inspect})"
    end
  end

  # Time duration/delta type.
  class Duration < TemporalType
    attr_reader :time_unit
    alias_method :tu, :time_unit

    def initialize(time_unit = "us")
      @time_unit = time_unit
    end

    def ==(other)
      if other.eql?(Duration)
        true
      elsif other.is_a?(Duration)
        time_unit == other.time_unit
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(time_unit: #{time_unit.inspect})"
    end
  end

  # A categorical encoding of a set of strings.
  class Categorical < DataType
  end

  # Type for wrapping arbitrary Ruby objects.
  class Object < DataType
  end

  # Type representing Null / None values.
  class Null < DataType
  end

  # Type representing Datatype values that could not be determined statically.
  class Unknown < DataType
  end

  # Nested list/array type.
  class List < NestedType
    attr_reader :inner

    def initialize(inner)
      @inner = Utils.rb_type_to_dtype(inner)
    end

    def ==(other)
      if other.eql?(List)
        true
      elsif other.is_a?(List)
        @inner.nil? || other.inner.nil? || @inner == other.inner
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(#{inner})"
    end
  end

  # Nested list/array type.
  class Array < NestedType
    attr_reader :width, :inner

    def initialize(width, inner)
      @width = width
      @inner = Utils.rb_type_to_dtype(inner)
    end

    # TODO check width?
    def ==(other)
      if other.eql?(Array)
        true
      elsif other.is_a?(Array)
        @inner.nil? || other.inner.nil? || @inner == other.inner
      else
        false
      end
    end

    # TODO add width?
    def to_s
      "#{self.class.name}(#{inner})"
    end
  end

  # Definition of a single field within a `Struct` DataType.
  class Field
    attr_reader :name, :dtype

    def initialize(name, dtype)
      @name = name
      @dtype = Utils.rb_type_to_dtype(dtype)
    end

    def inspect
      "#{self.class.name}(#{@name.inspect}, #{@dtype})"
    end
  end

  # Struct composite type.
  class Struct < NestedType
    attr_reader :fields

    def initialize(fields)
      if fields.is_a?(Hash)
        @fields = fields.map { |n, d| Field.new(n, d) }
      else
        @fields = fields
      end
    end

    def inspect
      "#{self.class.name}(#{@fields})"
    end

    def to_schema
      @fields.to_h { |f| [f.name, f.dtype] }
    end
  end
end

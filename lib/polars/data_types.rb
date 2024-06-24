module Polars
  # Base class for all Polars data types.
  class DataType
    # Return this DataType's fundamental/root type class.
    #
    # @return [Class]
    #
    # @example
    #   Polars::Datetime.new("ns").base_type
    #   # => Polars::Datetime
    # @example
    #   Polars::List.new(Polars::Int32).base_type
    #   # => Polars::List
    # @example
    #   Polars::Struct.new([Polars::Field.new("a", Polars::Int64), Polars::Field.new("b", Polars::Boolean)]).base_type
    #   # => Polars::Struct
    def self.base_type
      self
    end

    # Return this DataType's fundamental/root type class.
    #
    # @return [Class]
    def base_type
      is_a?(DataType) ? self.class : self
    end

    # Check if this DataType is the same as another DataType.
    #
    # @return [Boolean]
    def self.==(other)
      eql?(other) || other.is_a?(self)
    end

    # Check if this DataType is the same as another DataType.
    #
    # @return [Boolean]
    def ==(other)
      if other.is_a?(Class)
        is_a?(other)
      else
        other.instance_of?(self.class)
      end
    end

    # Check whether the data type is a numeric type.
    #
    # @return [Boolean]
    def self.numeric?
      self < NumericType
    end

    # Check whether the data type is a decimal type.
    #
    # @return [Boolean]
    def self.decimal?
      self == Decimal
    end

    # Check whether the data type is an integer type.
    #
    # @return [Boolean]
    def self.integer?
      self < IntegerType
    end

    # Check whether the data type is a signed integer type.
    #
    # @return [Boolean]
    def self.signed_integer?
      self < SignedIntegerType
    end

    # Check whether the data type is an unsigned integer type.
    #
    # @return [Boolean]
    def self.unsigned_integer?
      self < UnsignedIntegerType
    end

    # Check whether the data type is a float type.
    #
    # @return [Boolean]
    def self.float?
      self < FloatType
    end

    # Check whether the data type is a temporal type.
    #
    # @return [Boolean]
    def self.temporal?
      self < TemporalType
    end

    # Check whether the data type is a nested type.
    #
    # @return [Boolean]
    def self.nested?
      self < NestedType
    end

    [:numeric?, :decimal?, :integer?, :signed_integer?, :unsigned_integer?, :float?, :temporal?, :nested?].each do |v|
      define_method(v) do
        self.class.public_send(v)
      end
    end

    # Returns a string representing the data type.
    #
    # @return [String]
    def to_s
      self.class.name
    end

    # Returns a string representing the data type.
    #
    # @return [String]
    def inspect
      to_s
    end
  end

  # Base class for numeric data types.
  class NumericType < DataType
  end

  # Base class for integral data types.
  class IntegerType < NumericType
  end

  # @private
  IntegralType = IntegerType

  # Base class for signed integer data types.
  class SignedIntegerType < IntegerType
  end

  # Base class for unsigned integer data types.
  class UnsignedIntegerType < IntegerType
  end

  # Base class for float data types.
  class FloatType < NumericType
  end

  # Base class for temporal data types.
  class TemporalType < DataType
  end

  # Base class for nested data types.
  class NestedType < DataType
  end

  # 8-bit signed integer type.
  class Int8 < SignedIntegerType
  end

  # 16-bit signed integer type.
  class Int16 < SignedIntegerType
  end

  # 32-bit signed integer type.
  class Int32 < SignedIntegerType
  end

  # 64-bit signed integer type.
  class Int64 < SignedIntegerType
  end

  # 8-bit unsigned integer type.
  class UInt8 < UnsignedIntegerType
  end

  # 16-bit unsigned integer type.
  class UInt16 < UnsignedIntegerType
  end

  # 32-bit unsigned integer type.
  class UInt32 < UnsignedIntegerType
  end

  # 64-bit unsigned integer type.
  class UInt64 < UnsignedIntegerType
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
  class Decimal < NumericType
    attr_reader :precision, :scale

    def initialize(precision, scale)
      @precision = precision
      @scale = scale
    end

    def ==(other)
      if other.eql?(Decimal)
        true
      elsif other.is_a?(Decimal)
        precision == other.precision && scale == other.scale
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(precision: #{precision.inspect}, scale: #{scale.inspect})"
    end
  end

  # Boolean type.
  class Boolean < DataType
  end

  # UTF-8 encoded string type.
  class String < DataType
  end

  # @private
  # Allow Utf8 as an alias for String
  Utf8 = String

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
    def initialize(ordering = "physical")
      @ordering = ordering
    end
  end

  # A fixed set categorical encoding of a set of strings.
  #
  # NOTE: this is an experimental work-in-progress feature and may not work as expected.
  class Enum < DataType
    attr_reader :categories

    def initialize(categories)
      if !categories.is_a?(Series)
        categories = Series.new(categories)
      end

      if categories.empty?
        self.categories = Series.new("category", [], dtype: String)
        return
      end

      if categories.null_count > 0
        msg = "Enum categories must not contain null values"
        raise TypeError, msg
      end

      if (dtype = categories.dtype) != String
        msg = "Enum categories must be strings; found data of type #{dtype}"
        raise TypeError, msg
      end

      if categories.n_unique != categories.len
        duplicate = categories.filter(categories.is_duplicated)[0]
        msg = "Enum categories must be unique; found duplicate #{duplicate}"
        raise ArgumentError, msg
      end

      @categories = categories.rechunk.alias("category")
    end

    def ==(other)
      if other.eql?(Enum)
        true
      elsif other.is_a?(Enum)
        categories == other.categories
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(categories: #{categories.to_a.inspect})"
    end
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
    attr_reader :inner, :width

    def initialize(inner, width)
      if width.is_a?(DataType) || (width.is_a?(Class) && width < DataType)
        inner, width = width, inner
      end
      @inner = Utils.rb_type_to_dtype(inner) if inner
      @width = width
    end

    def ==(other)
      if other.eql?(Array)
        true
      elsif other.is_a?(Array)
        if @width != other.width
          false
        elsif @inner.nil? || other.inner.nil?
          true
        else
          @inner == other.inner
        end
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(#{inner}, width: #{width.inspect})"
    end
  end

  # Definition of a single field within a `Struct` DataType.
  class Field
    attr_reader :name, :dtype

    def initialize(name, dtype)
      @name = name
      @dtype = Utils.rb_type_to_dtype(dtype)
    end

    def ==(other)
      name == other.name && dtype == other.dtype
    end

    def to_s
      "#{self.class.name}(#{name.inspect}, #{dtype})"
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

    def ==(other)
      if other.eql?(Struct)
        true
      elsif other.is_a?(Struct)
        fields == other.fields
      else
        false
      end
    end

    def to_s
      "#{self.class.name}(#{fields.to_h { |f| [f.name, f.dtype] }})"
    end

    def to_schema
      @fields.to_h { |f| [f.name, f.dtype] }
    end
  end
end

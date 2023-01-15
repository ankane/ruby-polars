module Polars
  # Base class for all Polars data types.
  class DataType
  end

  # 8-bit signed integer type.
  class Int8 < DataType
  end

  # 16-bit signed integer type.
  class Int16 < DataType
  end

  # 32-bit signed integer type.
  class Int32 < DataType
  end

  # 64-bit signed integer type.
  class Int64 < DataType
  end

  # 8-bit unsigned integer type.
  class UInt8 < DataType
  end

  # 16-bit unsigned integer type.
  class UInt16 < DataType
  end

  # 32-bit unsigned integer type.
  class UInt32 < DataType
  end

  # 64-bit unsigned integer type.
  class UInt64 < DataType
  end

  # 32-bit floating point type.
  class Float32 < DataType
  end

  # 64-bit floating point type.
  class Float64 < DataType
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

  # Type representing Null / None values.
  class Null < DataType
  end

  # Type representing Datatype values that could not be determined statically.
  class Unknown < DataType
  end

  # TODO List

  # Calendar date type.
  class Date < DataType
  end

  # TODO DateTime

  # TODO Duration

  # Time of day type.
  class Time < DataType
  end

  # Type for wrapping arbitrary Python objects.
  class Object < DataType
  end

  # A categorical encoding of a set of strings.
  class Categorical < DataType
  end

  # TODO Field

  # TODO Struct
end

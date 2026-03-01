module Polars
  # @private
  class DataTypeGroup < Set
  end

  # @private
  SIGNED_INTEGER_DTYPES = DataTypeGroup.new(
    [
      Int8,
      Int16,
      Int32,
      Int64
    ]
  )
  # @private
  UNSIGNED_INTEGER_DTYPES = DataTypeGroup.new(
    [
      UInt8,
      UInt16,
      UInt32,
      UInt64
    ]
  )
  # @private
  INTEGER_DTYPES = (
    SIGNED_INTEGER_DTYPES | UNSIGNED_INTEGER_DTYPES
  )
  # @private
  FLOAT_DTYPES = DataTypeGroup.new([Float32, Float64])
  # @private
  NUMERIC_DTYPES = DataTypeGroup.new(
    FLOAT_DTYPES + INTEGER_DTYPES | [Decimal]
  )
end

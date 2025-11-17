module Polars
  # @private
  # Base class for all Polars errors.
  class Error < StandardError; end

  # @private
  # Exception raised when a specified column is not found.
  class ColumnNotFoundError < Error; end

  # @private
  # Exception raised when Polars could not perform an underlying computation.
  class ComputeError < Error; end

  # @private
  # Exception raised when a column name is duplicated.
  class DuplicateError < Error; end

  # @private
  # Exception raised when an operation is not allowed (or possible) against a given object or data structure.
  class InvalidOperationError < Error; end

  # @private
  # Exception raised when an operation cannot be performed on an empty data structure.
  class NoDataError < Error; end

  # @private
  # Exception raised when the given index is out of bounds.
  class OutOfBoundsError < Error; end

  # @private
  # Exception raised when an unexpected state causes a panic in the underlying Rust library.
  class PanicException < Error; end

  # @private
  # Exception raised when an unexpected schema mismatch causes an error.
  class SchemaError < Error; end

  # @private
  # Exception raised when a specified schema field is not found.
  class SchemaFieldNotFoundError < Error; end

  # @private
  # Exception raised when trying to perform operations on data structures with incompatible shapes.
  class ShapeError < Error; end

  # @private
  # Exception raised when an error occurs in the SQL interface.
  class SQLInterfaceError < Error; end

  # @private
  # Exception raised from the SQL interface when encountering invalid syntax.
  class SQLSyntaxError < Error; end

  # @private
  # Exception raised when string caches come from different sources.
  class StringCacheMismatchError < Error; end

  # @private
  # Exception raised when a specified Struct field is not found.
  class StructFieldNotFoundError < Error; end

  # @private
  # Exception raised when the number of returned rows does not match expectation.
  class RowsException < Error; end

  # @private
  # Exception raised when no rows are returned, but at least one row is expected.
  class NoRowsReturned < RowsException; end

  # @private
  # Exception raised when more rows than expected are returned.
  class TooManyRowsReturned < RowsException; end

  # @private
  class AssertionError < Error; end

  # @private
  class Todo < Error
    def message
      "not implemented yet"
    end
  end
end

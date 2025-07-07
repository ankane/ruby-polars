module Polars
  # @private
  # Base class for all Polars errors.
  class Error < StandardError; end

  # @private
  # Exception raised when an operation is not allowed (or possible) against a given object or data structure.
  class InvalidOperationError < Error; end

  # @private
  # Exception raised when an unsupported testing assert is made.
  class InvalidAssert < Error; end

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
  class ComputeError < Error; end

  # @private
  class DuplicateError < Error; end

  # @private
  class Todo < Error
    def message
      "not implemented yet"
    end
  end
end

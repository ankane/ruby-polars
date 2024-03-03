module Polars
  # @private
  class Error < StandardError; end

  # Exception raised when an unsupported testing assert is made.
  class InvalidAssert < Error; end

  # @private
  class RowsException < Error; end

  # @private
  class TooManyRowsReturned < RowsException; end

  # @private
  class NoRowsReturned < RowsException; end

  # @private
  class AssertionError < Error; end

  # @private
  class Todo < Error
    def message
      "not implemented yet"
    end
  end
end

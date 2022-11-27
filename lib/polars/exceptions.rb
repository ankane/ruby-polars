module Polars
  # @private
  class Error < StandardError; end

  # @private
  class RowsException < Error; end

  # @private
  class TooManyRowsReturned < RowsException; end

  # @private
  class NoRowsReturned < RowsException; end

  # @private
  class Todo < Error
    def message
      "not implemented yet"
    end
  end
end

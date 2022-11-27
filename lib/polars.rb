# ext
require "polars/polars"

# stdlib
require "date"

# modules
require "polars/expr_dispatch"
require "polars/batched_csv_reader"
require "polars/cat_expr"
require "polars/data_frame"
require "polars/date_time_expr"
require "polars/expr"
require "polars/functions"
require "polars/group_by"
require "polars/io"
require "polars/lazy_frame"
require "polars/lazy_functions"
require "polars/lazy_group_by"
require "polars/list_expr"
require "polars/meta_expr"
require "polars/series"
require "polars/slice"
require "polars/string_expr"
require "polars/struct_expr"
require "polars/utils"
require "polars/version"
require "polars/when"
require "polars/when_then"

module Polars
  # @private
  class Error < StandardError; end

  # @private
  class Todo < Error
    def message
      "not implemented yet"
    end
  end

  extend Functions
  extend IO
  extend LazyFunctions
end

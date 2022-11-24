# ext
require "polars/polars"

# modules
require "polars/array_expr"
require "polars/cat_expr"
require "polars/data_frame"
require "polars/date_time_expr"
require "polars/expr"
require "polars/functions"
require "polars/io"
require "polars/lazy_frame"
require "polars/lazy_functions"
require "polars/lazy_group_by"
require "polars/meta_expr"
require "polars/series"
require "polars/string_expr"
require "polars/struct_expr"
require "polars/utils"
require "polars/version"
require "polars/when"
require "polars/when_then"

module Polars
  class Error < StandardError; end

  extend Functions
  extend IO
  extend LazyFunctions
end

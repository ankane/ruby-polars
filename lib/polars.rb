# ext
begin
  require "polars/#{RUBY_VERSION.to_f}/polars"
rescue LoadError
  require "polars/polars"
end

# stdlib
require "date"
require "stringio"

# modules
require_relative "polars/expr_dispatch"
require_relative "polars/batched_csv_reader"
require_relative "polars/binary_expr"
require_relative "polars/binary_name_space"
require_relative "polars/cat_expr"
require_relative "polars/cat_name_space"
require_relative "polars/convert"
require_relative "polars/plot"
require_relative "polars/data_frame"
require_relative "polars/data_types"
require_relative "polars/date_time_expr"
require_relative "polars/date_time_name_space"
require_relative "polars/dynamic_group_by"
require_relative "polars/exceptions"
require_relative "polars/expr"
require_relative "polars/functions"
require_relative "polars/group_by"
require_relative "polars/io"
require_relative "polars/lazy_frame"
require_relative "polars/lazy_functions"
require_relative "polars/lazy_group_by"
require_relative "polars/list_expr"
require_relative "polars/list_name_space"
require_relative "polars/meta_expr"
require_relative "polars/rolling_group_by"
require_relative "polars/series"
require_relative "polars/slice"
require_relative "polars/string_expr"
require_relative "polars/string_name_space"
require_relative "polars/struct_expr"
require_relative "polars/struct_name_space"
require_relative "polars/utils"
require_relative "polars/version"
require_relative "polars/when"
require_relative "polars/when_then"

module Polars
  extend Convert
  extend Functions
  extend IO
  extend LazyFunctions
end

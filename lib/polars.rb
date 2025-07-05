# ext
begin
  require "polars/#{RUBY_VERSION.to_f}/polars"
rescue LoadError
  require "polars/polars"
end

# stdlib
require "bigdecimal"
require "date"
require "stringio"

# modules
require_relative "polars/expr_dispatch"
require_relative "polars/array_expr"
require_relative "polars/array_name_space"
require_relative "polars/batched_csv_reader"
require_relative "polars/binary_expr"
require_relative "polars/binary_name_space"
require_relative "polars/cat_expr"
require_relative "polars/cat_name_space"
require_relative "polars/config"
require_relative "polars/convert"
require_relative "polars/plot"
require_relative "polars/data_frame"
require_relative "polars/data_types"
require_relative "polars/data_type_group"
require_relative "polars/date_time_expr"
require_relative "polars/date_time_name_space"
require_relative "polars/dynamic_group_by"
require_relative "polars/exceptions"
require_relative "polars/expr"
require_relative "polars/functions/as_datatype"
require_relative "polars/functions/col"
require_relative "polars/functions/eager"
require_relative "polars/functions/lazy"
require_relative "polars/functions/len"
require_relative "polars/functions/lit"
require_relative "polars/functions/random"
require_relative "polars/functions/repeat"
require_relative "polars/functions/whenthen"
require_relative "polars/functions/aggregation/horizontal"
require_relative "polars/functions/aggregation/vertical"
require_relative "polars/functions/range/date_range"
require_relative "polars/functions/range/datetime_range"
require_relative "polars/functions/range/int_range"
require_relative "polars/functions/range/time_range"
require_relative "polars/group_by"
require_relative "polars/io/avro"
require_relative "polars/io/csv"
require_relative "polars/io/database"
require_relative "polars/io/delta"
require_relative "polars/io/ipc"
require_relative "polars/io/json"
require_relative "polars/io/ndjson"
require_relative "polars/io/parquet"
require_relative "polars/io/scan_options"
require_relative "polars/lazy_frame"
require_relative "polars/lazy_group_by"
require_relative "polars/list_expr"
require_relative "polars/list_name_space"
require_relative "polars/meta_expr"
require_relative "polars/name_expr"
require_relative "polars/rolling_group_by"
require_relative "polars/schema"
require_relative "polars/selectors"
require_relative "polars/series"
require_relative "polars/slice"
require_relative "polars/sql_context"
require_relative "polars/string_cache"
require_relative "polars/string_expr"
require_relative "polars/string_name_space"
require_relative "polars/struct_expr"
require_relative "polars/struct_name_space"
require_relative "polars/testing"
require_relative "polars/utils"
require_relative "polars/utils/constants"
require_relative "polars/utils/convert"
require_relative "polars/utils/parse"
require_relative "polars/utils/various"
require_relative "polars/utils/wrap"
require_relative "polars/version"
require_relative "polars/whenthen"

module Polars
  extend Convert
  extend Functions
  extend IO

  # @private
  F = self

  # @private
  N_INFER_DEFAULT = 100

  # @private
  class ArrowArrayStream
    def arrow_c_stream
      self
    end
  end

  # Return the number of threads in the Polars thread pool.
  #
  # @return [Integer]
  def self.thread_pool_size
    Plr.thread_pool_size
  end
end

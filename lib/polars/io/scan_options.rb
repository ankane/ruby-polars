module Polars
  module IO
    # @private
    class ScanOptions
      attr_reader :row_index, :pre_slice, :cast_options, :extra_columns, :missing_columns,
        :include_file_paths, :glob, :expand_paths, :hidden_file_prefix, :hive_partitioning, :hive_schema, :try_parse_hive_dates,
        :rechunk, :cache, :storage_options, :credential_provider, :retries, :column_mapping,
        :default_values, :deletion_files, :table_statistics, :row_count, :source_sizes, :resolve_heavy_sources

      def initialize(
        row_index: nil,
        pre_slice: nil,
        cast_options: nil,
        extra_columns: "raise",
        missing_columns: "raise",
        include_file_paths: nil,
        glob: true,
        expand_paths: true,
        hidden_file_prefix: nil,
        hive_partitioning: nil,
        hive_schema: nil,
        try_parse_hive_dates: true,
        rechunk: false,
        cache: true,
        storage_options: nil,
        credential_provider: nil,
        column_mapping: nil,
        default_values: nil,
        deletion_files: nil,
        table_statistics: nil,
        row_count: nil,
        source_sizes: nil,
        resolve_heavy_sources: nil
      )
        @row_index = row_index
        @pre_slice = pre_slice
        @cast_options = cast_options
        @extra_columns = extra_columns
        @missing_columns = missing_columns
        @include_file_paths = include_file_paths
        @glob = glob
        @expand_paths = extend_paths
        @hidden_file_prefix = hidden_file_prefix
        @hive_partitioning = hive_partitioning
        @hive_schema = hive_schema
        @try_parse_hive_dates = try_parse_hive_dates
        @rechunk = rechunk
        @cache = cache
        @storage_options = storage_options
        @credential_provider = credential_provider
        @column_mapping = column_mapping
        @default_values = default_values
        @deletion_files = deletion_files
        @table_statistics = table_statistics
        @row_count = row_count
        @source_sizes = source_sizes
        @resolve_heavy_sources = resolve_heavy_sources
      end
    end
  end
end

module Polars
  module IO
    class ScanOptions
      attr_reader :row_index, :pre_slice, :cast_options, :extra_columns, :missing_columns,
        :include_file_paths, :glob, :hive_partitioning, :hive_schema, :try_parse_hive_dates,
        :rechunk, :cache, :storage_options, :credential_provider, :retries, :column_mapping, :deletion_files

      def initialize(
        row_index: nil,
        pre_slice: nil,
        cast_options: nil,
        extra_columns: "raise",
        missing_columns: "raise",
        include_file_paths: nil,
        glob: true,
        hive_partitioning: nil,
        hive_schema: nil,
        try_parse_hive_dates: true,
        rechunk: false,
        cache: true,
        storage_options: nil,
        credential_provider: nil,
        retries: 2,
        column_mapping: nil,
        deletion_files: nil
      )
        @row_index = row_index
        @pre_slice = pre_slice
        @cast_options = cast_options
        @extra_columns = extra_columns
        @missing_columns = missing_columns
        @include_file_paths = include_file_paths
        @glob = glob
        @hive_partitioning = hive_partitioning
        @hive_schema = hive_schema
        @try_parse_hive_dates = try_parse_hive_dates
        @rechunk = rechunk
        @cache = cache
        @storage_options = storage_options
        @credential_provider = credential_provider
        @retries = retries
        @column_mapping = column_mapping
        @deletion_files = deletion_files
      end
    end
  end
end

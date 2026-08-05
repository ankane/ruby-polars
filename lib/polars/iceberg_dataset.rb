module Polars
  # @private
  class IcebergDataset
    def initialize(
      source,
      snapshot_id:,
      storage_options:
    )
      @source = source
      @snapshot_id = snapshot_id
      @storage_options = storage_options
    end

    def to_lazyframe
      scan = @source.scan(snapshot_id: @snapshot_id)
      files = scan.plan_files

      table = scan.table
      snapshot = scan.snapshot
      schema = snapshot ? table.schema_by_id(snapshot.schema_id) : table.current_schema

      sources = files.map { |v| v.file.file_path }

      column_mapping = [
        "iceberg-column-mapping",
        schema
      ]

      deletion_files = [
        "iceberg-position-delete",
        files.map.with_index
          .select { |v, i| v.delete_files.any? }
          .to_h { |v, i| [i, v.delete_files.map(&:file_path)] }
      ]

      scan_options = {
        schema: Schema.new(schema),
        cast_options: Polars::ScanCastOptions._default_iceberg,
        missing_columns: "insert",
        extra_columns: "ignore",
        storage_options: @storage_options,
        _column_mapping: column_mapping,
        _deletion_files: deletion_files
      }

      Polars.scan_parquet(sources, **scan_options)
    end
  end
end

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
      # for iceberg < 0.1.3
      if !@source.respond_to?(:scan)
        return @source.to_polars(snapshot_id: @snapshot_id, storage_options: @storage_options)
      end

      scan = @source.scan(snapshot_id: @snapshot_id)
      files = scan.plan_files

      table = scan.table
      snapshot = scan.snapshot
      schema = snapshot ? table.schema_by_id(snapshot[:schema_id]) : table.current_schema

      if files.empty?
        # TODO improve
        schema =
          schema.fields.to_h do |field|
            dtype =
              case field[:type]
              when "int"
                Polars::Int32
              when "long"
                Polars::Int64
              when "double"
                Polars::Float64
              when "string"
                Polars::String
              when "timestamp"
                Polars::Datetime
              else
                raise Todo
              end

            [field[:name], dtype]
          end

        LazyFrame.new(schema: schema)
      else
        sources = files.map { |v| v[:data_file_path] }

        column_mapping = [
          "iceberg-column-mapping",
          arrow_schema(schema)
        ]

        deletion_files = [
          "iceberg-position-delete",
          files.map.with_index
            .select { |v, i| v[:deletes].any? }
            .to_h { |v, i| [i, v[:deletes].map { |d| d[:file_path] }] }
        ]

        scan_options = {
          storage_options: @storage_options,
          cast_options: Polars::ScanCastOptions._default_iceberg,
          allow_missing_columns: true,
          extra_columns: "ignore",
          _column_mapping: column_mapping,
          _deletion_files: deletion_files
        }

        Polars.scan_parquet(sources, **scan_options)
      end
    end

    private

    def arrow_schema(schema)
      fields =
        schema.fields.map do |field|
          type =
            case field[:type]
            when "boolean"
              "boolean"
            when "int"
              "int32"
            when "long"
              "int64"
            when "float"
              "float32"
            when "double"
              "float64"
            else
              raise Todo
            end

          {
            name: field[:name],
            type: type,
            nullable: !field[:required],
            metadata: {
              "PARQUET:field_id" => field[:id].to_s
            }
          }
        end

      {fields: fields}
    end
  end
end

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
      schema = snapshot ? table.schema_by_id(snapshot[:schema_id]) : table.current_schema

      if files.empty?
        # TODO improve
        schema =
          schema.fields.to_h do |field|
            dtype =
              case field[:type]
              when "boolean"
                Polars::Boolean
              when "int"
                Polars::Int32
              when "long"
                Polars::Int64
              when "float"
                Polars::Float32
              when "double"
                Polars::Float64
              when "decimal"
                Polars::Decimal.new(field[:precision], field[:scale])
              when "string"
                Polars::String
              when "binary"
                Polars::Binary
              when "date"
                Polars::Date
              when "timestamp"
                Polars::Datetime.new("us")
              when "timestamp_ns"
                Polars::Datetime.new("ns")
              when "timestamptz"
                Polars::Datetime.new("us", "+00:00")
              when "timestamptz_ns"
                Polars::Datetime.new("ns", "+00:00")
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
          missing_columns: "insert",
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
            when "decimal"
              "decimal"
            when "string"
              "string"
            when "binary"
              "large_binary"
            when "date"
              "date32"
            when "timestamp"
              time_unit = "us"
              "timestamp"
            when "timestamp_ns"
              time_unit = "ns"
              "timestamp"
            when "timestamptz"
              time_unit = "us"
              time_zone = "+00:00"
              "timestamp"
            when "timestamptz_ns"
              time_unit = "ns"
              time_zone = "+00:00"
              "timestamp"
            else
              raise Todo
            end

          arrow_field = {
            name: field[:name],
            type: type,
            nullable: !field[:required],
            metadata: {
              "PARQUET:field_id" => field[:id].to_s
            }
          }
          if type == "decimal"
            arrow_field[:precision] = field[:precision]
            arrow_field[:scale] = field[:scale]
          end
          arrow_field[:time_unit] = time_unit if time_unit
          arrow_field[:time_zone] = time_zone if time_zone
          arrow_field
        end

      {fields: fields}
    end
  end
end

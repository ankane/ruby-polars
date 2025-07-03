# frozen_string_literal: true

require 'time'

module Polars
  module Catalog
    class Unity
      class Client
        def initialize(api_client)
          @client = api_client
        end

        def list_catalogs
          response = @client.get('/api/2.1/unity-catalog/catalogs')
          (response['catalogs'] || []).map do |c|
            Models::CatalogInfo.new(
              name: c['name'],
              comment: c['comment'],
              storage_location: c['storage_root'],
              created_at: c['created_at'],
              created_by: c['created_by'],
              updated_at: c['updated_at'],
              updated_by: c['updated_by']
            )
          end
        end

        def list_namespaces(catalog_name)
          response = @client.get('/api/2.1/unity-catalog/schemas', { catalog_name: catalog_name })
          (response['schemas'] || []).map do |s|
            Models::NamespaceInfo.new(
              name: s['name'],
              comment: s['comment'],
              storage_location: s['storage_root'],
              created_at: s['created_at'],
              created_by: s['created_by'],
              updated_at: s['updated_at'],
              updated_by: s['updated_by']
            )
          end
        end

        def list_tables(catalog_name, namespace)
          response = @client.get('/api/2.1/unity-catalog/tables', { catalog_name: catalog_name, schema_name: namespace })
          (response['tables'] || []).map do |t|
            Models::TableInfo.new(
              name: t['name'],
              comment: t['comment'],
              table_id: t['table_id'],
              table_type: t['table_type'],
              storage_location: t['storage_location'],
              data_source_format: t['data_source_format'],
              columns: (t['columns'] || []).map do |col|
                Models::ColumnInfo.new(
                  name: col['name'],
                  type_name: col['type_name'],
                  type_text: col['type_text'],
                  type_json: col['type_json'],
                  position: col['position'],
                  comment: col['comment'],
                  partition_index: col['partition_index']
                )
              end,
              created_at: t['created_at'],
              created_by: t['created_by'],
              updated_at: t['updated_at'],
              updated_by: t['updated_by']
            )
          end
        end

        def get_table_info(catalog_name, namespace, table_name)
          full_name = "#{catalog_name}.#{namespace}.#{table_name}"
          response = @client.get("/api/2.1/unity-catalog/tables/#{full_name}")
          t = response
          Models::TableInfo.new(
            name: t['name'],
            comment: t['comment'],
            table_id: t['table_id'],
            table_type: t['table_type'],
            storage_location: t['storage_location'],
            data_source_format: t['data_source_format'],
            columns: (t['columns'] || []).map do |col|
              Models::ColumnInfo.new(
                name: col['name'],
                type_name: col['type_name'],
                type_text: col['type_text'],
                type_json: col['type_json'],
                position: col['position'],
                comment: col['comment'],
                partition_index: col['partition_index']
              )
            end,
            created_at: t['created_at'],
            created_by: t['created_by'],
            updated_at: t['updated_at'],
            updated_by: t['updated_by']
          )
        end

        def scan_table(
          catalog_name,
          namespace,
          table_name,
          delta_table_version: nil,
          delta_table_options: nil,
          storage_options: nil
        )
          table_info = get_table_info(catalog_name, namespace, table_name)
          if table_info.storage_location.nil? || table_info.data_source_format.nil?
            raise ArgumentError, "cannot scan table: missing storage_location or data_source_format"
          end

          if ["DELTA", "DELTASHARING"].include?(table_info.data_source_format)
            storage_options = Hash::new if storage_options.nil?
            Polars.scan_delta(
              table_info.storage_location,
              version: delta_table_version,
              delta_table_options: delta_table_options,
              storage_options: storage_options
            )
          else
            case table_info.data_source_format
            when "PARQUET"
              Polars.scan_parquet(table_info.storage_location)
            when "CSV"
              Polars.scan_csv(table_info.storage_location)
            when "JSON"
              Polars.scan_ndjson(table_info.storage_location)
            else
              raise NotImplementedError, "scan_table for format #{table_info.data_source_format} is not supported in this client yet."
            end
          end
        end
      end
    end
  end
end

# frozen_string_literal: true

module Polars
  module Catalog
    class Unity
      module Models
        class CatalogInfo
          attr_accessor :name, :comment, :storage_location, :created_at, :created_by, :updated_at, :updated_by

          def initialize(name:, comment:, storage_location:, created_at:, created_by:, updated_at:, updated_by:)
            @name = name
            @comment = comment
            @storage_location = storage_location
            @created_at = Time.parse(created_at) if created_at.is_a?(String)
            @created_by = created_by
            @updated_at = Time.parse(updated_at) if updated_at.is_a?(String)
            @updated_by = updated_by
          end
        end

        class TableInfo
          attr_accessor :name, :comment, :table_id, :table_type, :storage_location, :data_source_format, :columns, :created_at, :created_by, :updated_at, :updated_by

          def initialize(name:, comment:, table_id:, table_type:, storage_location:, data_source_format:, columns:, created_at:, created_by:, updated_at:, updated_by:)
            @name = name
            @comment = comment
            @table_id = table_id
            @table_type = table_type
            @storage_location = storage_location
            @data_source_format = data_source_format
            @columns = columns
            @created_at = created_at
            @created_by = created_by
            @updated_at = updated_at
            @updated_by = updated_by
          end
        end

        class ColumnInfo
          attr_accessor :name, :type_name, :type_text, :type_json, :position, :comment, :partition_index

          def initialize(name:, type_name:, type_text:, type_json:, position:, comment:, partition_index:)
            @name = name
            @type_name = type_name
            @type_text = type_text
            @type_json = type_json
            @position = position
            @comment = comment
            @partition_index = partition_index
          end
        end

        class NamespaceInfo
          attr_accessor :name, :comment, :storage_location, :created_at, :created_by, :updated_at, :updated_by

          def initialize(name:, comment:, storage_location:, created_at:, created_by:, updated_at:, updated_by:)
            @name = name
            @comment = comment
            @storage_location = storage_location
            @created_at = created_at
            @created_by = created_by
            @updated_at = updated_at
            @updated_by = updated_by
          end
        end
      end
    end
  end
end

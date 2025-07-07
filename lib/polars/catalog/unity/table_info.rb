module Polars
  class Catalog
    module Unity
      # Information for a catalog table.
      TableInfo =
        ::Struct.new(
          :name,
          :comment,
          :table_id,
          :table_type,
          :storage_location,
          :data_source_format,
          :columns,
          :properties,
          :created_at,
          :created_by,
          :updated_at,
          :updated_by,
          keyword_init: true
        )

      class TableInfo
        # Get the native polars schema of this table.
        #
        # @note
        #   This functionality is considered **unstable**. It may be changed
        #   at any point without it being considered a breaking change.
        #
        # @return [Schema]
        def get_polars_schema
          if columns.nil?
            return nil
          end

          schema = Schema.new(check_dtypes: false)

          columns.each do |column_info|
            if schema[column_info.name]
              msg = "duplicate column name: #{column_info.name}"
              raise DuplicateError, msg
            end
            schema[column_info.name] = column_info.get_polars_dtype
          end

          schema
        end
      end
    end
  end
end

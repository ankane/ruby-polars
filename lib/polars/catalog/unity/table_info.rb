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
    end
  end
end

module Polars
  class Catalog
    module Unity
      # Information for a column within a catalog table.
      ColumnInfo =
        ::Struct.new(
          :name,
          :type_name,
          :type_text,
          :type_json,
          :position,
          :comment,
          :partition_index,
          keyword_init: true
        )
    end
  end
end

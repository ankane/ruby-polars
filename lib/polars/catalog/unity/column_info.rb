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

      class ColumnInfo
        # Get the native polars datatype of this column.
        #
        # @note
        #   This functionality is considered **unstable**. It may be changed
        #   at any point without it being considered a breaking change.
        #
        # @return [Object]
        def get_polars_dtype
          RbCatalogClient.type_json_to_polars_type(type_json)
        end
      end
    end
  end
end

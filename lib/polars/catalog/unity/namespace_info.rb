module Polars
  class Catalog
    module Unity
      # Information for a namespace within a catalog.
      #
      # This is also known by the name "schema" in unity catalog terminology.
      NamespaceInfo =
        ::Struct.new(
          :name,
          :comment,
          :properties,
          :storage_location,
          :created_at,
          :created_by,
          :updated_at,
          :updated_by,
          keyword_init: true
        )
    end
  end
end

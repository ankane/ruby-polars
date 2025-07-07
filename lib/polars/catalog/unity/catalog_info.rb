module Polars
  class Catalog
    module Unity
      CatalogInfo =
        ::Struct.new(
          :name,
          :comment,
          :properties,
          :options,
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

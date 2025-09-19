module Polars
  module IO
    # Lazily read from an Apache Iceberg table.
    #
    # @param source [Object]
    #   A Iceberg Ruby table, or a direct path to the metadata.
    # @param snapshot_id [Integer]
    #   The snapshot ID to scan from.
    # @param storage_options [Hash]
    #   Extra options for the storage backends.
    #
    # @return [LazyFrame]
    def scan_iceberg(
      source,
      snapshot_id: nil,
      storage_options: nil
    )
      require "iceberg"

      unless source.is_a?(Iceberg::Table)
        raise Todo
      end

      dataset =
        IcebergDataset.new(
          source,
          snapshot_id:,
          storage_options:
        )

      dataset.to_lazyframe
    end
  end
end

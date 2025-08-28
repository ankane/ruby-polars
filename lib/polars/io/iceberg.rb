module Polars
  module IO
    # Lazily read from an Apache Iceberg table.
    #
    # @param source [Object]
    #   A Iceberg Ruby table, or a direct path to the metadata.
    # @param snapshot_id [Integer]
    #   The snapshot ID to scan from.
    #
    # @return [LazyFrame]
    def scan_iceberg(source, snapshot_id: nil)
      require "iceberg"

      unless source.is_a?(Iceberg::Table)
        raise Todo
      end

      source.to_polars(snapshot_id:)
    end
  end
end

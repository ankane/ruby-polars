module Polars
  module IO
    # Lazily read from an Apache Iceberg table.
    #
    # @param source
    #   A Iceberg Ruby table, or a direct path to the metadata.
    #
    # @return [LazyFrame]
    def scan_iceberg(source)
      require "iceberg"

      unless source.is_a?(Iceberg::Table)
        raise Todo
      end

      source.to_polars
    end
  end
end

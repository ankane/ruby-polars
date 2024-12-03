module Polars
  module IO
    # Reads into a DataFrame from a Delta lake table.
    #
    # @param source [Object]
    #   DeltaTable or a Path or URI to the root of the Delta lake table.
    #
    # @return [DataFrame]
    def read_delta(source)
      dl_tbl = _get_delta_lake_table(source)
      dl_tbl.to_polars
    end

    # Lazily read from a Delta lake table.
    #
    # @param source [Object]
    #   DeltaTable or a Path or URI to the root of the Delta lake table.
    #
    # @return [LazyFrame]
    def scan_delta(source)
      dl_tbl = _get_delta_lake_table(source)
      dl_tbl.to_polars(eager: false)
    end

    private

    def _get_delta_lake_table(table_path)
      _check_if_delta_available

      if table_path.is_a?(DeltaLake::Table)
        return table_path
      end

      dl_tbl = DeltaLake::Table.new(table_path)
      dl_tbl
    end

    def _check_if_delta_available
      if !defined?(DeltaLake)
        raise Error, "Delta Lake not available"
      end
    end
  end
end

module Polars
  module IO
    # Reads into a DataFrame from a Delta lake table.
    #
    # @param source [Object]
    #   DeltaTable or a Path or URI to the root of the Delta lake table.
    # @param version [Object]
    #   Numerical version or timestamp version of the Delta lake table.
    # @param columns [Array]
    #   Columns to select. Accepts a list of column names.
    # @param rechunk [Boolean]
    #   Make sure that all columns are contiguous in memory by
    #   aggregating the chunks into a single array.
    # @param storage_options [Hash]
    #   Extra options for the storage backends supported by `deltalake-rb`.
    # @param delta_table_options [Hash]
    #   Additional keyword arguments while reading a Delta lake Table.
    #
    # @return [DataFrame]
    def read_delta(
      source,
      version: nil,
      columns: nil,
      rechunk: false,
      storage_options: nil,
      delta_table_options: nil
    )
      dl_tbl =
        _get_delta_lake_table(
          source,
          version: version,
          storage_options: storage_options,
          delta_table_options: delta_table_options
        )

      dl_tbl.to_polars(columns: columns, rechunk: rechunk)
    end

    # Lazily read from a Delta lake table.
    #
    # @param source [Object]
    #   DeltaTable or a Path or URI to the root of the Delta lake table.
    # @param version [Object]
    #   Numerical version or timestamp version of the Delta lake table.
    # @param storage_options [Hash]
    #   Extra options for the storage backends supported by `deltalake-rb`.
    # @param delta_table_options [Hash]
    #   Additional keyword arguments while reading a Delta lake Table.
    #
    # @return [LazyFrame]
    def scan_delta(
      source,
      version: nil,
      storage_options: nil,
      delta_table_options: nil
    )
      dl_tbl =
        _get_delta_lake_table(
          source,
          version: version,
          storage_options: storage_options,
          delta_table_options: delta_table_options
        )

      dl_tbl.to_polars(eager: false)
    end

    private

    def _resolve_delta_lake_uri(table_uri, strict: true)
      require "uri"

      parsed_result = URI(table_uri)

      resolved_uri =
        if parsed_result.scheme == ""
          Utils.normalize_filepath(table_uri)
        else
          table_uri
        end

      resolved_uri
    end

    def _get_delta_lake_table(
      table_path,
      version: nil,
      storage_options: nil,
      delta_table_options: nil
    )
      _check_if_delta_available

      if table_path.is_a?(DeltaLake::Table)
        return table_path
      end
      delta_table_options ||= {}
      resolved_uri = _resolve_delta_lake_uri(table_path)
      if !version.is_a?(::String) && !version.is_a?(::Time)
        dl_tbl =
          DeltaLake::Table.new(
            resolved_uri,
            version: version,
            storage_options: storage_options,
            **delta_table_options
          )
      else
        dl_tbl =
          DeltaLake::Table.new(
            resolved_uri,
            storage_options: storage_options,
            **delta_table_options
          )
        dl_tbl.load_as_version(version)
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

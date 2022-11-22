module Polars
  module IO
    def read_csv(file, has_header: true)
      _prepare_file_arg(file) do |data|
        DataFrame._read_csv(data, has_header: has_header)
      end
    end

    def read_parquet(file)
      _prepare_file_arg(file) do |data|
        DataFrame._read_parquet(data)
      end
    end

    def read_json(file)
      DataFrame._read_json(file)
    end

    def read_ndjson(file)
      DataFrame._read_ndjson(file)
    end

    private

    def _prepare_file_arg(file)
      if file.is_a?(String) && file =~ /\Ahttps?:\/\//
        raise ArgumentError, "use URI(...) for remote files"
      end

      if defined?(URI) && file.is_a?(URI)
        require "open-uri"

        file = URI.open(file)
      end

      yield file
    end
  end
end

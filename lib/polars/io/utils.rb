module Polars
  module IO
    private

    def get_sources(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source, check_not_directory: false)
      elsif Utils.is_path_or_str_sequence(source)
        source = source.map { |s| Utils.normalize_filepath(s, check_not_directory: false) }
      end
      unless source.is_a?(::Array)
        source = [source]
      end
      source
    end
  end
end

module Polars
  module IO
    # Read into a DataFrame from a JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [DataFrame]
    def read_json(source)
      DataFrame._read_json(source)
    end
  end
end

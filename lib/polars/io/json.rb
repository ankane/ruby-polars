module Polars
  module IO
    # Read into a DataFrame from a JSON file.
    #
    # @param source [Object]
    #   Path to a file or a file-like object.
    #
    # @return [DataFrame]
    def read_json(source)
      if Utils.pathlike?(source)
        source = Utils.normalize_filepath(source)
      end

      rbdf = RbDataFrame.read_json(source)
      Utils.wrap_df(rbdf)
    end
  end
end

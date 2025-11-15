module Polars
  module Utils
    def self.serialize_polars_object(serializer, file)
      serialize_to_bytes = lambda do
        buf = StringIO.new
        serializer.(buf)
        buf.string
      end

      if file.nil?
        serialize_to_bytes.call
      elsif pathlike?(file)
        file = normalize_filepath(file)
        serializer.(file)
        nil
      else
        serializer.(file)
        nil
      end
    end
  end
end

module Polars
  module Utils
    def self.serialize_polars_object(serializer, file)
      serialize_to_bytes = lambda do
        buf = StringIO.new
        serializer.(buf)
        buf.string
      end

      if file.nil?
        return serialize_to_bytes.call
      end

      raise Todo
    end
  end
end

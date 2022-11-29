module Polars
  # Series.struct namespace.
  class StructNameSpace
    include ExprDispatch

    self._accessor = "struct"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Retrieve one of the fields of this `Struct` as a new Series.
    #
    # @return [Series]
    def [](item)
      if item.is_a?(Integer)
        field(fields[item])
      elsif item.is_a?(String)
        field(item)
      else
        raise ArgumentError, "expected type Integer or String, got #{item.class.name}"
      end
    end

    # Convert this Struct Series to a DataFrame.
    #
    # @return [DataFrame]
    def to_frame
      Utils.wrap_df(_s.struct_to_frame)
    end

    # Get the names of the fields.
    #
    # @return [Array]
    def fields
      if _s.nil?
        []
      else
        _s.struct_fields
      end
    end

    # Retrieve one of the fields of this `Struct` as a new Series.
    #
    # @param name [String]
    #   Name of the field
    #
    # @return [Series]
    def field(name)
      super
    end

    # Rename the fields of the struct.
    #
    # @param names [Array]
    #   New names in the order of the struct's fields
    #
    # @return [Series]
    def rename_fields(names)
      super
    end
  end
end

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
      elsif item.is_a?(::String)
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
    #
    # @example
    #   s = Polars::Series.new([{"a" => 1, "b" => 2}, {"a" => 3, "b" => 4}])
    #   s.struct.fields
    #   # => ["a", "b"]
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
    #
    # @example
    #   s = Polars::Series.new([{"a" => 1, "b" => 2}, {"a" => 3, "b" => 4}])
    #   s.struct.field("a")
    #   # =>
    #   # shape: (2,)
    #   # Series: 'a' [i64]
    #   # [
    #   #         1
    #   #         3
    #   # ]
    def field(name)
      super
    end

    # Rename the fields of the struct.
    #
    # @param names [Array]
    #   New names in the order of the struct's fields
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([{"a" => 1, "b" => 2}, {"a" => 3, "b" => 4}])
    #   s.struct.fields
    #   # => ["a", "b"]
    #
    # @example
    #   s = s.struct.rename_fields(["c", "d"])
    #   s.struct.fields
    #   # => ["c", "d"]
    def rename_fields(names)
      super
    end

    # Get the struct definition as a name/dtype schema dict.
    #
    # @return [Object]
    #
    # @example
    #   s = Polars::Series.new([{"a" => 1, "b" => 2}, {"a" => 3, "b" => 4}])
    #   s.struct.schema
    #   # => {"a"=>Polars::Int64, "b"=>Polars::Int64}
    def schema
      if _s.nil?
        {}
      else
        _s.dtype.to_schema
      end
    end

    # Convert this struct Series to a DataFrame with a separate column for each field.
    #
    # @return [DataFrame]
    #
    # @example
    #   s = Polars::Series.new([{"a" => 1, "b" => 2}, {"a" => 3, "b" => 4}])
    #   s.struct.unnest
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ 2   │
    #   # │ 3   ┆ 4   │
    #   # └─────┴─────┘
    def unnest
      Utils.wrap_df(_s.struct_unnest)
    end
  end
end

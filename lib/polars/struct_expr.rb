module Polars
  # Namespace for struct related expressions.
  class StructExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Retrieve one of the fields of this `Struct` as a new Series.
    #
    # @return [Expr]
    def [](item)
      if item.is_a?(::String)
        field(item)
      elsif item.is_a?(Integer)
        Utils.wrap_expr(_rbexpr.struct_field_by_index(item))
      else
        raise ArgumentError, "expected type Integer or String, got #{item.class.name}"
      end
    end

    # Retrieve one of the fields of this `Struct` as a new Series.
    #
    # @param name [String]
    #   Name of the field
    #
    # @return [Expr]
    #
    # @example
    #   df = (
    #     Polars::DataFrame.new(
    #       {
    #         "int" => [1, 2],
    #         "str" => ["a", "b"],
    #         "bool" => [true, nil],
    #         "list" => [[1, 2], [3]]
    #       }
    #     )
    #     .to_struct("my_struct")
    #     .to_frame
    #   )
    #   df.select(Polars.col("my_struct").struct.field("str"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ str │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ a   │
    #   # │ b   │
    #   # └─────┘
    def field(name)
      Utils.wrap_expr(_rbexpr.struct_field_by_name(name))
    end

    # Rename the fields of the struct.
    #
    # @param names [Array]
    #   New names in the order of the struct's fields
    #
    # @return [Expr]
    #
    # @example
    #   df = (
    #     Polars::DataFrame.new(
    #       {
    #         "int" => [1, 2],
    #         "str" => ["a", "b"],
    #         "bool" => [true, nil],
    #         "list" => [[1, 2], [3]]
    #       }
    #     )
    #     .to_struct("my_struct")
    #     .to_frame
    #   )
    #   df = df.with_column(
    #     Polars.col("my_struct").struct.rename_fields(["INT", "STR", "BOOL", "LIST"])
    #   )
    #   df.select(Polars.col("my_struct").struct.field("INT"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ INT │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # └─────┘
    def rename_fields(names)
      Utils.wrap_expr(_rbexpr.struct_rename_fields(names))
    end
  end
end

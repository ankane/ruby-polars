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
    # @param more_names [Array]
    #   Additional struct field names.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "aaa" => [1, 2],
    #       "bbb" => ["ab", "cd"],
    #       "ccc" => [true, nil],
    #       "ddd" => [[1, 2], [3]]
    #     }
    #   ).select(Polars.struct("aaa", "bbb", "ccc", "ddd").alias("struct_col"))
    #   df.select(Polars.col("struct_col").struct.field("bbb"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ bbb │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ ab  │
    #   # │ cd  │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.col("struct_col").struct.field("aaa", "bbb"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ aaa ┆ bbb │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ str │
    #   # ╞═════╪═════╡
    #   # │ 1   ┆ ab  │
    #   # │ 2   ┆ cd  │
    #   # └─────┴─────┘
    def field(name, *more_names)
      if more_names.any?
        name = (name.is_a?(::String) ? [name] : name) + more_names
      end
      if name.is_a?(::Array)
        return Utils.wrap_expr(_rbexpr.struct_multiple_fields(name))
      end

      Utils.wrap_expr(_rbexpr.struct_field_by_name(name))
    end

    # Expand the struct into its individual fields.
    #
    # Alias for `Expr.struct.field("*")`.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "aaa" => [1, 2],
    #       "bbb" => ["ab", "cd"],
    #       "ccc" => [true, nil],
    #       "ddd" => [[1, 2], [3]]
    #     }
    #   ).select(Polars.struct("aaa", "bbb", "ccc", "ddd").alias("struct_col"))
    #   df.select(Polars.col("struct_col").struct.unnest)
    #   # =>
    #   # shape: (2, 4)
    #   # ┌─────┬─────┬──────┬───────────┐
    #   # │ aaa ┆ bbb ┆ ccc  ┆ ddd       │
    #   # │ --- ┆ --- ┆ ---  ┆ ---       │
    #   # │ i64 ┆ str ┆ bool ┆ list[i64] │
    #   # ╞═════╪═════╪══════╪═══════════╡
    #   # │ 1   ┆ ab  ┆ true ┆ [1, 2]    │
    #   # │ 2   ┆ cd  ┆ null ┆ [3]       │
    #   # └─────┴─────┴──────┴───────────┘
    def unnest
      field("*")
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
    #   df = df.with_columns(
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

    # Convert this struct to a string column with json values.
    #
    # @return [Expr]
    #
    # @example
    #   Polars::DataFrame.new(
    #     {"a" => [{"a" => [1, 2], "b" => [45]}, {"a" => [9, 1, 3], "b" => nil}]}
    #   ).with_columns(Polars.col("a").struct.json_encode.alias("encoded"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌──────────────────┬────────────────────────┐
    #   # │ a                ┆ encoded                │
    #   # │ ---              ┆ ---                    │
    #   # │ struct[2]        ┆ str                    │
    #   # ╞══════════════════╪════════════════════════╡
    #   # │ {[1, 2],[45]}    ┆ {"a":[1,2],"b":[45]}   │
    #   # │ {[9, 1, 3],null} ┆ {"a":[9,1,3],"b":null} │
    #   # └──────────────────┴────────────────────────┘
    def json_encode
      Utils.wrap_expr(_rbexpr.struct_json_encode)
    end

    # Add or overwrite fields of this struct.
    #
    # This is similar to `with_columns` on `DataFrame`.
    #
    # @param exprs [Array]
    #   Field(s) to add, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names, other
    #   non-expression inputs are parsed as literals.
    # @param named_exprs [Hash]
    #   Additional fields to add, specified as keyword arguments.
    #   The columns will be renamed to the keyword used.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "coords" => [{"x" => 1, "y" => 4}, {"x" => 4, "y" => 9}, {"x" => 9, "y" => 16}],
    #       "multiply" => [10, 2, 3]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("coords").struct.with_fields(
    #       Polars.field("x").sqrt,
    #       y_mul: Polars.field("y") * Polars.col("multiply")
    #     )
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────┬──────────┐
    #   # │ coords      ┆ multiply │
    #   # │ ---         ┆ ---      │
    #   # │ struct[3]   ┆ i64      │
    #   # ╞═════════════╪══════════╡
    #   # │ {1.0,4,40}  ┆ 10       │
    #   # │ {2.0,9,18}  ┆ 2        │
    #   # │ {3.0,16,48} ┆ 3        │
    #   # └─────────────┴──────────┘
    def with_fields(
      *exprs,
      **named_exprs
    )
      structify = ENV.fetch("POLARS_AUTO_STRUCTIFY", 0).to_i != 0

      rbexprs = Utils.parse_into_list_of_expressions(
        *exprs, **named_exprs, __structify: structify
      )

      Utils.wrap_expr(_rbexpr.struct_with_fields(rbexprs))
    end
  end
end

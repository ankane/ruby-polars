module Polars
  # Namespace for expressions that operate on expression names.
  class NameExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # Keep the original root name of the expression.
    #
    # @note
    #   Due to implementation constraints, this method can only be called as the last
    #   expression in a chain.
    #
    # @return [Expr]
    #
    # @example Prevent errors due to potential duplicate column names.
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2],
    #       "b" => [3, 4]
    #     }
    #   )
    #   df.select((Polars.lit(10) / Polars.all).name.keep)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌──────┬──────────┐
    #   # │ a    ┆ b        │
    #   # │ ---  ┆ ---      │
    #   # │ f64  ┆ f64      │
    #   # ╞══════╪══════════╡
    #   # │ 10.0 ┆ 3.333333 │
    #   # │ 5.0  ┆ 2.5      │
    #   # └──────┴──────────┘
    #
    # @example Undo an alias operation.
    #   df.with_columns((Polars.col("a") * 9).alias("c").name.keep)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ a   ┆ b   │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 9   ┆ 3   │
    #   # │ 18  ┆ 4   │
    #   # └─────┴─────┘
    def keep
      Utils.wrap_expr(_rbexpr.name_keep)
    end

    # Rename the output of an expression by mapping a function over the root name.
    #
    # @return [Expr]
    #
    # @example Remove a common suffix and convert to lower case.
    #   df = Polars::DataFrame.new(
    #     {
    #       "A_reverse" => [3, 2, 1],
    #       "B_reverse" => ["z", "y", "x"]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.all.reverse.name.map { |c| c.delete_suffix("_reverse").downcase }
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌───────────┬───────────┬─────┬─────┐
    #   # │ A_reverse ┆ B_reverse ┆ a   ┆ b   │
    #   # │ ---       ┆ ---       ┆ --- ┆ --- │
    #   # │ i64       ┆ str       ┆ i64 ┆ str │
    #   # ╞═══════════╪═══════════╪═════╪═════╡
    #   # │ 3         ┆ z         ┆ 1   ┆ x   │
    #   # │ 2         ┆ y         ┆ 2   ┆ y   │
    #   # │ 1         ┆ x         ┆ 3   ┆ z   │
    #   # └───────────┴───────────┴─────┴─────┘
    def map(&function)
      Utils.wrap_expr(_rbexpr.name_map(function))
    end

    # Add a prefix to the root column name of the expression.
    #
    # @param prefix [Object]
    #   Prefix to add to the root column name.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.all.reverse.name.prefix("reverse_"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬───────────┬───────────┐
    #   # │ a   ┆ b   ┆ reverse_a ┆ reverse_b │
    #   # │ --- ┆ --- ┆ ---       ┆ ---       │
    #   # │ i64 ┆ str ┆ i64       ┆ str       │
    #   # ╞═════╪═════╪═══════════╪═══════════╡
    #   # │ 1   ┆ x   ┆ 3         ┆ z         │
    #   # │ 2   ┆ y   ┆ 2         ┆ y         │
    #   # │ 3   ┆ z   ┆ 1         ┆ x         │
    #   # └─────┴─────┴───────────┴───────────┘
    def prefix(prefix)
      Utils.wrap_expr(_rbexpr.name_prefix(prefix))
    end

    # Add a suffix to the root column name of the expression.
    #
    # @param suffix [Object]
    #   Suffix to add to the root column name.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.all.reverse.name.suffix("_reverse"))
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬───────────┬───────────┐
    #   # │ a   ┆ b   ┆ a_reverse ┆ b_reverse │
    #   # │ --- ┆ --- ┆ ---       ┆ ---       │
    #   # │ i64 ┆ str ┆ i64       ┆ str       │
    #   # ╞═════╪═════╪═══════════╪═══════════╡
    #   # │ 1   ┆ x   ┆ 3         ┆ z         │
    #   # │ 2   ┆ y   ┆ 2         ┆ y         │
    #   # │ 3   ┆ z   ┆ 1         ┆ x         │
    #   # └─────┴─────┴───────────┴───────────┘
    def suffix(suffix)
      Utils.wrap_expr(_rbexpr.name_suffix(suffix))
    end

    # Make the root column name lowercase.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "ColX" => [1, 2, 3],
    #       "ColY" => ["x", "y", "z"],
    #     }
    #   )
    #   df.with_columns(Polars.all.name.to_lowercase)
    #   # =>
    #   # shape: (3, 4)
    #   # ┌──────┬──────┬──────┬──────┐
    #   # │ ColX ┆ ColY ┆ colx ┆ coly │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ str  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╪══════╡
    #   # │ 1    ┆ x    ┆ 1    ┆ x    │
    #   # │ 2    ┆ y    ┆ 2    ┆ y    │
    #   # │ 3    ┆ z    ┆ 3    ┆ z    │
    #   # └──────┴──────┴──────┴──────┘
    def to_lowercase
      Utils.wrap_expr(_rbexpr.name_to_lowercase)
    end

    # Make the root column name uppercase.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "ColX" => [1, 2, 3],
    #       "ColY" => ["x", "y", "z"]
    #     }
    #   )
    #   df.with_columns(Polars.all.name.to_uppercase)
    #   # =>
    #   # shape: (3, 4)
    #   # ┌──────┬──────┬──────┬──────┐
    #   # │ ColX ┆ ColY ┆ COLX ┆ COLY │
    #   # │ ---  ┆ ---  ┆ ---  ┆ ---  │
    #   # │ i64  ┆ str  ┆ i64  ┆ str  │
    #   # ╞══════╪══════╪══════╪══════╡
    #   # │ 1    ┆ x    ┆ 1    ┆ x    │
    #   # │ 2    ┆ y    ┆ 2    ┆ y    │
    #   # │ 3    ┆ z    ┆ 3    ┆ z    │
    #   # └──────┴──────┴──────┴──────┘
    def to_uppercase
      Utils.wrap_expr(_rbexpr.name_to_uppercase)
    end

    # Add a prefix to all field names of a struct.
    #
    # @note
    #   This only takes effect for struct columns.
    #
    # @param prefix [String]
    #   Prefix to add to the field name.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => {"a" => 1, "b" => 2}})
    #   df.select(Polars.col("x").name.prefix_fields("prefix_")).schema
    #   # => Polars::Schema({"x"=>Polars::Struct({"prefix_a"=>Polars::Int64, "prefix_b"=>Polars::Int64})})
    def prefix_fields(prefix)
      Utils.wrap_expr(_rbexpr.name_prefix_fields(prefix))
    end

    # Replace matching regex/literal substring in the name with a new value.
    #
    # @param pattern [String]
    #   A valid regular expression pattern, compatible with the [regex crate](https://docs.rs/regex/latest/regex/).
    # @param value [String]
    #   String that will replace the matched substring.
    # @param literal [Boolean]
    #   Treat `pattern` as a literal string, not a regex.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "n_foo" => [1, 2, 3],
    #       "n_bar" => ["x", "y", "z"]
    #     }
    #   )
    #   df.select(Polars.all.name.replace("^n_", "col_"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────┬─────────┐
    #   # │ col_foo ┆ col_bar │
    #   # │ ---     ┆ ---     │
    #   # │ i64     ┆ str     │
    #   # ╞═════════╪═════════╡
    #   # │ 1       ┆ x       │
    #   # │ 2       ┆ y       │
    #   # │ 3       ┆ z       │
    #   # └─────────┴─────────┘
    def replace(pattern, value, literal: false)
      Utils.wrap_expr(_rbexpr.name_replace(pattern, value, literal))
    end

    # Add a suffix to all field names of a struct.
    #
    # @note
    #   This only takes effect for struct columns.
    #
    # @param suffix [String]
    #   Suffix to add to the field name.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => {"a" => 1, "b" => 2}})
    #   df.select(Polars.col("x").name.suffix_fields("_suffix")).schema
    #   # => Polars::Schema({"x"=>Polars::Struct({"a_suffix"=>Polars::Int64, "b_suffix"=>Polars::Int64})})
    def suffix_fields(suffix)
      Utils.wrap_expr(_rbexpr.name_suffix_fields(suffix))
    end
  end
end

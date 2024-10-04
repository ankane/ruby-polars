module Polars
  module Selectors
    # @private
    class SelectorProxy < Expr
      def initialize(
        expr,
        name:,
        parameters: nil
      )
        self._rbexpr = expr._rbexpr
      end

      def ~
        if Utils.is_selector(self)
          inverted = Selectors.all - self
          # inverted._repr_override = f"~{self!r}"
        else
          inverted = ~as_expr
        end
        inverted
      end

      def -(other)
        if Utils.is_selector(other)
          SelectorProxy.new(
            meta._as_selector.meta._selector_sub(other),
            parameters: {"self" => self, "other" => other},
            name: "sub"
          )
        else
          as_expr - other
        end
      end

      def as_expr
        Expr._from_rbexpr(_rbexpr)
      end
    end

    # @private
    def self._selector_proxy_(...)
      SelectorProxy.new(...)
    end

    # Select all columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dt" => [Date.new(1999, 12, 31), Date.new(2024, 1, 1)],
    #       "value" => [1_234_500, 5_000_555]
    #     },
    #     schema_overrides: {"value" => Polars::Int32}
    #   )
    #
    # @example Select all columns, casting them to string:
    #   df.select(Polars.cs.all.cast(Polars::String))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬─────────┐
    #   # │ dt         ┆ value   │
    #   # │ ---        ┆ ---     │
    #   # │ str        ┆ str     │
    #   # ╞════════════╪═════════╡
    #   # │ 1999-12-31 ┆ 1234500 │
    #   # │ 2024-01-01 ┆ 5000555 │
    #   # └────────────┴─────────┘
    #
    # @example Select all columns *except* for those matching the given dtypes:
    #   df.select(Polars.cs.all - Polars.cs.numeric)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────────┐
    #   # │ dt         │
    #   # │ ---        │
    #   # │ date       │
    #   # ╞════════════╡
    #   # │ 1999-12-31 │
    #   # │ 2024-01-01 │
    #   # └────────────┘
    def self.all
      _selector_proxy_(F.all, name: "all")
    end

    # Select all numeric columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [0, 0]
    #     },
    #     schema_overrides: {"bar" => Polars::Int16, "baz" => Polars::Float32, "zap" => Polars::UInt8},
    #   )
    #
    # @example Match all numeric columns:
    #   df.select(Polars.cs.numeric)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ bar ┆ baz ┆ zap │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i16 ┆ f32 ┆ u8  │
    #   # ╞═════╪═════╪═════╡
    #   # │ 123 ┆ 2.0 ┆ 0   │
    #   # │ 456 ┆ 5.5 ┆ 0   │
    #   # └─────┴─────┴─────┘
    #
    # @example Match all columns *except* for those that are numeric:
    #   df.select(~Polars.cs.numeric)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ x   │
    #   # │ y   │
    #   # └─────┘
    def self.numeric
      _selector_proxy_(F.col(NUMERIC_DTYPES), name: "numeric")
    end
  end

  def self.cs
    Polars::Selectors
  end
end

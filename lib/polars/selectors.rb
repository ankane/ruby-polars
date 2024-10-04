module Polars
  module Selectors
    def self.numeric
      _selector_proxy_(F.col(NUMERIC_DTYPES), name: "numeric")
    end

    def self._selector_proxy_(...)
      SelectorProxy.new(...)
    end

    class SelectorProxy < Expr
      def initialize(
        expr,
        name:,
        parameters: nil
      )
        self._rbexpr = expr._rbexpr
      end
    end
  end

  def self.cs
    Polars::Selectors
  end
end

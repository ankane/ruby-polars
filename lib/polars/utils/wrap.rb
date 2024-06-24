module Polars
  module Utils
    def self.wrap_df(df)
      DataFrame._from_rbdf(df)
    end

    def self.wrap_ldf(ldf)
      LazyFrame._from_rbldf(ldf)
    end

    def self.wrap_s(s)
      Series._from_rbseries(s)
    end

    def self.wrap_expr(rbexpr)
      Expr._from_rbexpr(rbexpr)
    end
  end
end

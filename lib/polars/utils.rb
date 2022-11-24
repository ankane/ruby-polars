module Polars
  module Utils
    DTYPE_TEMPORAL_UNITS = ["ns", "us", "ms"]

    def self.wrap_s(s)
      Series._from_rbseries(s)
    end

    def self.wrap_df(df)
      DataFrame._from_rbdf(df)
    end

    def self.wrap_expr(rbexpr)
      Expr._from_rbexpr(rbexpr)
    end

    def self.col(name)
      Polars.col(name)
    end

    def self.selection_to_rbexpr_list(exprs)
      if exprs.is_a?(String) || exprs.is_a?(Expr) || exprs.is_a?(Series)
        exprs = [exprs]
      end

      exprs.map { |e| expr_to_lit_or_expr(e, str_to_lit: false)._rbexpr }
    end

    def self.expr_to_lit_or_expr(expr, str_to_lit: true)
      if expr.is_a?(String) && !str_to_lit
        col(expr)
      elsif expr.is_a?(Integer) || expr.is_a?(Float) || expr.is_a?(String) || expr.is_a?(Series) || expr.nil?
        lit(expr)
      elsif expr.is_a?(Expr)
        expr
      else
        raise ArgumentError, "did not expect value #{expr} of type #{expr.class.name}, maybe disambiguate with Polars.lit or Polars.col"
      end
    end

    def self.lit(value)
      Polars.lit(value)
    end

    def self.format_path(path)
      File.expand_path(path)
    end

    # TODO fix
    def self.is_polars_dtype(data_type)
      true
    end

    # TODO fix
    def self.rb_type_to_dtype(dtype)
      dtype.to_s
    end

    def self.scale_bytes(sz, to:)
      scaling_factor = {
          "b" => 1,
          "k" => 1024,
          "m" => 1024 ** 2,
          "g" => 1024 ** 3,
          "t" => 1024 ** 4,
      }[to[0]]
      if scaling_factor > 1
        sz / scaling_factor.to_f
      else
        sz
      end
    end
  end
end

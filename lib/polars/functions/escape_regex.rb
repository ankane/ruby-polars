module Polars
  module Functions
    # Escapes string regex meta characters.
    #
    # @param s [String]
    #   The string whose meta characters will be escaped.
    #
    # @return [String]
    def escape_regex(s)
      if s.is_a?(Expr)
        msg = "escape_regex function is unsupported for `Expr`, you may want use `Expr.str.escape_regex` instead"
        raise TypeError, msg
      elsif !s.is_a?(::String)
        msg = "escape_regex function supports only `String` type, got `#{s.class.name}`"
        raise TypeError, msg
      end

      Plr.escape_regex(s)
    end
  end
end

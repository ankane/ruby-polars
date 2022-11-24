module Polars
  class StringExpr
    attr_accessor :_rbexpr

    def initialize(expr)
      self._rbexpr = expr._rbexpr
    end

    # def strptime
    # end

    def lengths
      Utils.wrap_expr(_rbexpr.str_lengths)
    end

    # def n_chars
    # end

    # def concat
    # end

    # def to_uppercase
    # end

    # def to_lowercase
    # end

    # def strip
    # end

    # def lstrip
    # end

    # def rstrip
    # end

    # def zfill
    # end

    # def ljust
    # end

    # def rjust
    # end

    def contains(pattern, literal: false)
      Utils.wrap_expr(_rbexpr.str_contains(pattern, literal))
    end

    # def ends_with
    # end

    # def starts_with
    # end

    # def json_path_match
    # end

    # def decode
    # end

    # def encode
    # end

    # def extract
    # end

    # def extract_all
    # end

    # def count_match
    # end

    # def split
    # end

    # def split_exact
    # end

    # def splitn
    # end

    # def replace
    # end

    # def replace_all
    # end

    # def slice
    # end
  end
end

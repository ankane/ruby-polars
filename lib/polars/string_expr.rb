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

    def n_chars
      Utils.wrap_expr(_rbexpr.str_n_chars)
    end

    def concat(delimiter = "-")
      Utils.wrap_expr(_rbexpr.str_concat(delimiter))
    end

    def to_uppercase
      Utils.wrap_expr(_rbexpr.str_to_uppercase)
    end

    def to_lowercase
      Utils.wrap_expr(_rbexpr.str_to_lowercase)
    end

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

    def ends_with(sub)
      Utils.wrap_expr(_rbexpr.str_ends_with(sub))
    end

    def starts_with(sub)
      Utils.wrap_expr(_rbexpr.str_starts_with(sub))
    end

    # def json_path_match
    # end

    # def decode
    # end

    # def encode
    # end

    def extract(pattern, group_index: 1)
      Utils.wrap_expr(_rbexpr.str_extract(pattern, group_index))
    end

    def extract_all(pattern)
      Utils.wrap_expr(_rbexpr.str_extract_all(pattern))
    end

    def count_match(pattern)
      Utils.wrap_expr(_rbexpr.count_match(pattern))
    end

    def split(by, inclusive: false)
      if inclusive
        Utils.wrap_expr(_rbexpr.str_split_inclusive(by))
      else
        Utils.wrap_expr(_rbexpr.str_split(by))
      end
    end

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

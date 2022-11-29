module Polars
  # Series.str namespace.
  class StringNameSpace
    include ExprDispatch

    self._accessor = "str"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # def strptime
    # end

    # def lengths
    # end

    # def n_chars
    # end

    # def concat
    # end

    # def contains
    # end

    # def ends_with
    # end

    # def starts_with
    # end

    # def decode
    # end

    # def encode
    # end

    # def json_path_match
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

    # Remove leading and trailing whitespace.
    #
    # @param matches [String, nil]
    #   An optional single character that should be trimmed
    #
    # @return [Series]
    def strip(matches = nil)
      super
    end

    # Remove leading whitespace.
    #
    # @param matches [String, nil]
    #   An optional single character that should be trimmed
    #
    # @return [Series]
    def lstrip(matches = nil)
      super
    end

    # Remove trailing whitespace.
    #
    # @param matches [String, nil]
    #   An optional single character that should be trimmed
    #
    # @return [Series]
    def rstrip(matches = nil)
      super
    end

    # Fills the string with zeroes.
    #
    # Return a copy of the string left filled with ASCII '0' digits to make a string
    # of length width.
    #
    # A leading sign prefix ('+'/'-') is handled by inserting the padding after the
    # sign character rather than before. The original string is returned if width is
    # less than or equal to `s.length`.
    #
    # @param alignment [Integer]
    #   Fill the value up to this length.
    #
    # @return [Series]
    def zfill(alignment)
      super
    end

    # Return the string left justified in a string of length `width`.
    #
    # Padding is done using the specified `fillchar`. The original string is
    # returned if `width` is less than or equal to``s.length`.
    #
    # @param width [Integer]
    #   Justify left to this length.
    # @param fillchar [String]
    #   Fill with this ASCII character.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", ["cow", "monkey", nil, "hippopotamus"])
    #   s.str.ljust(8, "*")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [str]
    #   # [
    #   #         "cow*****"
    #   #         "monkey**"
    #   #         null
    #   #         "hippopotamus"
    #   # ]
    def ljust(width, fillchar = " ")
      super
    end

    # Return the string right justified in a string of length `width`.
    #
    # Padding is done using the specified `fillchar`. The original string is
    # returned if `width` is less than or equal to `s.length`.
    #
    # @param width [Integer]
    #   Justify right to this length.
    # @param fillchar [String]
    #   Fill with this ASCII character.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("a", ["cow", "monkey", nil, "hippopotamus"])
    #   s.str.rjust(8, "*")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'a' [str]
    #   # [
    #   #         "*****cow"
    #   #         "**monkey"
    #   #         null
    #   #         "hippopotamus"
    #   # ]
    def rjust(width, fillchar = " ")
      super
    end

    # Modify the strings to their lowercase equivalent.
    #
    # @return [Series]
    def to_lowercase
      super
    end

    # Modify the strings to their uppercase equivalent.
    #
    # @return [Series]
    def to_uppercase
      super
    end

    # Create subslices of the string values of a Utf8 Series.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the string.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("s", ["pear", nil, "papaya", "dragonfruit"])
    #   s.str.slice(-3)
    #   # =>
    #   # shape: (4,)
    #   # Series: 's' [str]
    #   # [
    #   #         "ear"
    #   #         null
    #   #         "aya"
    #   #         "uit"
    #   # ]
    #
    # @example Using the optional `length` parameter
    #   s.str.slice(4, 3)
    #   # =>
    #   # shape: (4,)
    #   # Series: 's' [str]
    #   # [
    #   #         ""
    #   #         null
    #   #         "ya"
    #   #         "onf"
    #   # ]
    def slice(offset, length = nil)
      s = Utils.wrap_s(_s)
      s.to_frame.select(Polars.col(s.name).str.slice(offset, length)).to_series
    end
  end
end

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

    # Check if strings in Series contain a substring that matches a regex.
    #
    # @param pattern [String]
    #   A valid regex pattern.
    # @param literal [Boolean]
    #   Treat pattern as a literal string.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["Crab", "cat and dog", "rab$bit", nil])
    #   s.str.contains("cat|bit")
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         true
    #   #         null
    #   # ]
    #
    # @example
    #   s.str.contains("rab$", literal: true)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [bool]
    #   # [
    #   #         false
    #   #         false
    #   #         true
    #   #         null
    #   # ]
    def contains(pattern, literal: false)
      super
    end

    # Check if string values end with a substring.
    #
    # @param sub [String]
    #   Suffix substring.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("fruits", ["apple", "mango", nil])
    #   s.str.ends_with("go")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'fruits' [bool]
    #   # [
    #   #         false
    #   #         true
    #   #         null
    #   # ]
    def ends_with(sub)
      super
    end

    # Check if string values start with a substring.
    #
    # @param sub [String]
    #   Prefix substring.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("fruits", ["apple", "mango", nil])
    #   s.str.starts_with("app")
    #   # =>
    #   # shape: (3,)
    #   # Series: 'fruits' [bool]
    #   # [
    #   #         true
    #   #         false
    #   #         null
    #   # ]
    def starts_with(sub)
      super
    end

    # def decode
    # end

    # def encode
    # end

    # def json_path_match
    # end

    # Extract the target capture group from provided patterns.
    #
    # @param pattern [String]
    #   A valid regex pattern
    # @param group_index [Integer]
    #   Index of the targeted capture group.
    #   Group 0 mean the whole pattern, first group begin at index 1
    #   Default to the first capture group
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["123 bla 45 asd", "xyz 678 910t"]})
    #   df.select([Polars.col("foo").str.extract('(\d+)')])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ 123 │
    #   # ├╌╌╌╌╌┤
    #   # │ 678 │
    #   # └─────┘
    def extract(pattern, group_index: 1)
      super
    end

    # Extracts all matches for the given regex pattern.
    #
    # Extract each successive non-overlapping regex match in an individual string as
    # an array
    #
    # @param pattern [String]
    #   A valid regex pattern
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("foo", ["123 bla 45 asd", "xyz 678 910t"])
    #   s.str.extract_all('(\d+)')
    #   # =>
    #   # shape: (2,)
    #   # Series: 'foo' [list]
    #   # [
    #   #         ["123", "45"]
    #   #         ["678", "910"]
    #   # ]
    def extract_all(pattern)
      super
    end

    # Count all successive non-overlapping regex matches.
    #
    # @param pattern [String]
    #   A valid regex pattern
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("foo", ["123 bla 45 asd", "xyz 678 910t"])
    #   s.str.count_match('\d')
    #   # =>
    #   # shape: (2,)
    #   # Series: 'foo' [u32]
    #   # [
    #   #         5
    #   #         6
    #   # ]
    def count_match(pattern)
      super
    end

    # Split the string by a substring.
    #
    # @param by [String]
    #   Substring to split by.
    # @param inclusive [Boolean]
    #   If true, include the split character/string in the results.
    #
    # @return [Series]
    def split(by, inclusive: false)
      super
    end

    # Split the string by a substring using `n` splits.
    #
    # Results in a struct of `n+1` fields.
    #
    # If it cannot make `n` splits, the remaining field elements will be null.
    #
    # @param by [String]
    #   Substring to split by.
    # @param n [Integer]
    #   Number of splits to make.
    # @param inclusive [Boolean]
    #   If true, include the split character/string in the results.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => ["a_1", nil, "c", "d_4"]})
    #   df["x"].str.split_exact("_", 1).alias("fields")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'fields' [struct[2]]
    #   # [
    #   #         {"a","1"}
    #   #         {null,null}
    #   #         {"c",null}
    #   #         {"d","4"}
    #   # ]
    #
    # @example Split string values in column x in exactly 2 parts and assign each part to a new column.
    #   df["x"]
    #     .str.split_exact("_", 1)
    #     .struct.rename_fields(["first_part", "second_part"])
    #     .alias("fields")
    #     .to_frame
    #     .unnest("fields")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────────┬─────────────┐
    #   # │ first_part ┆ second_part │
    #   # │ ---        ┆ ---         │
    #   # │ str        ┆ str         │
    #   # ╞════════════╪═════════════╡
    #   # │ a          ┆ 1           │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null       ┆ null        │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ c          ┆ null        │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ d          ┆ 4           │
    #   # └────────────┴─────────────┘
    def split_exact(by, n, inclusive: false)
      super
    end

    # Split the string by a substring, restricted to returning at most `n` items.
    #
    # If the number of possible splits is less than `n-1`, the remaining field
    # elements will be null. If the number of possible splits is `n-1` or greater,
    # the last (nth) substring will contain the remainder of the string.
    #
    # @param by [String]
    #   Substring to split by.
    # @param n [Integer]
    #   Max number of items to return.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => ["foo bar", nil, "foo-bar", "foo bar baz"]})
    #   df["s"].str.splitn(" ", 2).alias("fields")
    #   # =>
    #   # shape: (4,)
    #   # Series: 'fields' [struct[2]]
    #   # [
    #   #         {"foo","bar"}
    #   #         {null,null}
    #   #         {"foo-bar",null}
    #   #         {"foo","bar baz"}
    #   # ]
    #
    # @example Split string values in column s in exactly 2 parts and assign each part to a new column.
    #   df["s"]
    #     .str.splitn(" ", 2)
    #     .struct.rename_fields(["first_part", "second_part"])
    #     .alias("fields")
    #     .to_frame
    #     .unnest("fields")
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────────┬─────────────┐
    #   # │ first_part ┆ second_part │
    #   # │ ---        ┆ ---         │
    #   # │ str        ┆ str         │
    #   # ╞════════════╪═════════════╡
    #   # │ foo        ┆ bar         │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ null       ┆ null        │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ foo-bar    ┆ null        │
    #   # ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    #   # │ foo        ┆ bar baz     │
    #   # └────────────┴─────────────┘
    def splitn(by, n)
      s = Utils.wrap_s(_s)
      s.to_frame.select(Polars.col(s.name).str.splitn(by, n)).to_series
    end

    # Replace first matching regex/literal substring with a new string value.
    #
    # @param pattern [String]
    #   A valid regex pattern.
    # @param value [String]
    #   Substring to replace.
    # @param literal [Boolean]
    #   Treat pattern as a literal string.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["123abc", "abc456"])
    #   s.str.replace('abc\b', "ABC")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         "123ABC"
    #   #         "abc456"
    #   # ]
    def replace(pattern, value, literal: false)
      super
    end

    # Replace all matching regex/literal substrings with a new string value.
    #
    # @param pattern [String]
    #   A valid regex pattern.
    # @param value [String]
    #   Substring to replace.
    # @param literal [Boolean]
    #   Treat pattern as a literal string.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::Series.new(["abcabc", "123a123"])
    #   df.str.replace_all("a", "-")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         "-bc-bc"
    #   #         "123-123"
    #   # ]
    def replace_all(pattern, value, literal: false)
      super
    end

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

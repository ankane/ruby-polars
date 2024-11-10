module Polars
  # Series.str namespace.
  class StringNameSpace
    include ExprDispatch

    self._accessor = "str"

    # @private
    def initialize(series)
      self._s = series._s
    end

    # Convert a Utf8 column into a Date column.
    #
    # @param format [String]
    #   Format to use for conversion. Refer to the
    #   [chrono crate documentation](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   for the full specification. Example: `"%Y-%m-%d"`.
    #   If set to nil (default), the format is inferred from the data.
    # @param strict [Boolean]
    #   Raise an error if any conversion fails.
    # @param exact [Boolean]
    #   Require an exact format match. If false, allow the format to match anywhere
    #   in the target string.
    # @param cache [Boolean]
    #   Use a cache of unique, converted dates to apply the conversion.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["2020/01/01", "2020/02/01", "2020/03/01"])
    #   s.str.to_date
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [date]
    #   # [
    #   #         2020-01-01
    #   #         2020-02-01
    #   #         2020-03-01
    #   # ]
    def to_date(format = nil, strict: true, exact: true, cache: true)
      super
    end

    # Convert a Utf8 column into a Datetime column.
    #
    # @param format [String]
    #   Format to use for conversion. Refer to the
    #   [chrono crate documentation](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   for the full specification. Example: `"%Y-%m-%d %H:%M:%S"`.
    #   If set to nil (default), the format is inferred from the data.
    # @param time_unit ["us", "ns", "ms"]
    #   Unit of time for the resulting Datetime column. If set to nil (default),
    #   the time unit is inferred from the format string if given, eg:
    #   `"%F %T%.3f"` => `Datetime("ms")`. If no fractional second component is
    #   found, the default is `"us"`.
    # @param time_zone [String]
    #   Time zone for the resulting Datetime column.
    # @param strict [Boolean]
    #   Raise an error if any conversion fails.
    # @param exact [Boolean]
    #   Require an exact format match. If false, allow the format to match anywhere
    #   in the target string.
    # @param cache [Boolean]
    #   Use a cache of unique, converted datetimes to apply the conversion.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["2020-01-01 01:00Z", "2020-01-01 02:00Z"])
    #   s.str.to_datetime("%Y-%m-%d %H:%M%#z")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [datetime[μs, UTC]]
    #   # [
    #   #         2020-01-01 01:00:00 UTC
    #   #         2020-01-01 02:00:00 UTC
    #   # ]
    def to_datetime(
      format = nil,
      time_unit: nil,
      time_zone: nil,
      strict: true,
      exact: true,
      cache: true,
      ambiguous: "raise"
    )
      super
    end

    # Convert a Utf8 column into a Time column.
    #
    # @param format [String]
    #   Format to use for conversion. Refer to the
    #   [chrono crate documentation](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   for the full specification. Example: `"%H:%M:%S"`.
    #   If set to nil (default), the format is inferred from the data.
    # @param strict [Boolean]
    #   Raise an error if any conversion fails.
    # @param cache [Boolean]
    #   Use a cache of unique, converted times to apply the conversion.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["01:00", "02:00", "03:00"])
    #   s.str.to_time("%H:%M")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [time]
    #   # [
    #   #         01:00:00
    #   #         02:00:00
    #   #         03:00:00
    #   # ]
    def to_time(format = nil, strict: true, cache: true)
      super
    end

    # Parse a Series of dtype Utf8 to a Date/Datetime Series.
    #
    # @param datatype [Symbol]
    #   `:date`, `:dateime`, or `:time`.
    # @param fmt [String]
    #   Format to use, refer to the
    #   [chrono strftime documentation](https://docs.rs/chrono/latest/chrono/format/strftime/index.html)
    #   for specification. Example: `"%y-%m-%d"`.
    # @param strict [Boolean]
    #   Raise an error if any conversion fails.
    # @param exact [Boolean]
    #   - If true, require an exact format match.
    #   - If false, allow the format to match anywhere in the target string.
    # @param cache [Boolean]
    #   Use a cache of unique, converted dates to apply the datetime conversion.
    #
    # @return [Series]
    #
    # @example Dealing with a consistent format:
    #   s = Polars::Series.new(["2020-01-01 01:00Z", "2020-01-01 02:00Z"])
    #   s.str.strptime(Polars::Datetime, "%Y-%m-%d %H:%M%#z")
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [datetime[μs, UTC]]
    #   # [
    #   #         2020-01-01 01:00:00 UTC
    #   #         2020-01-01 02:00:00 UTC
    #   # ]
    #
    # @example Dealing with different formats.
    #   s = Polars::Series.new(
    #     "date",
    #     [
    #       "2021-04-22",
    #       "2022-01-04 00:00:00",
    #       "01/31/22",
    #       "Sun Jul  8 00:34:60 2001"
    #     ]
    #   )
    #   s.to_frame.select(
    #     Polars.coalesce(
    #       Polars.col("date").str.strptime(Polars::Date, "%F", strict: false),
    #       Polars.col("date").str.strptime(Polars::Date, "%F %T", strict: false),
    #       Polars.col("date").str.strptime(Polars::Date, "%D", strict: false),
    #       Polars.col("date").str.strptime(Polars::Date, "%c", strict: false)
    #     )
    #   ).to_series
    #   # =>
    #   # shape: (4,)
    #   # Series: 'date' [date]
    #   # [
    #   #         2021-04-22
    #   #         2022-01-04
    #   #         2022-01-31
    #   #         2001-07-08
    #   # ]
    def strptime(datatype, fmt = nil, strict: true, exact: true, cache: true)
      super
    end

    # Get length of the string values in the Series (as number of bytes).
    #
    # @return [Series]
    #
    # @note
    #   The returned lengths are equal to the number of bytes in the UTF8 string. If you
    #   need the length in terms of the number of characters, use `n_chars` instead.
    #
    # @example
    #   s = Polars::Series.new(["Café", nil, "345", "東京"])
    #   s.str.lengths
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         5
    #   #         null
    #   #         3
    #   #         6
    #   # ]
    def lengths
      super
    end

    # Get length of the string values in the Series (as number of chars).
    #
    # @return [Series]
    #
    # @note
    #   If you know that you are working with ASCII text, `lengths` will be
    #   equivalent, and faster (returns length in terms of the number of bytes).
    #
    # @example
    #   s = Polars::Series.new(["Café", nil, "345", "東京"])
    #   s.str.n_chars
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [u32]
    #   # [
    #   #         4
    #   #         null
    #   #         3
    #   #         2
    #   # ]
    def n_chars
      super
    end

    # Vertically concat the values in the Series to a single string value.
    #
    # @param delimiter [String]
    #   The delimiter to insert between consecutive string values.
    #
    # @return [Series]
    #
    # @example
    #   Polars::Series.new([1, nil, 2]).str.join("-")
    #   # =>
    #   # shape: (1,)
    #   # Series: '' [str]
    #   # [
    #   #         "1-2"
    #   # ]
    #
    # @example
    #   Polars::Series.new([1, nil, 2]).str.join("-", ignore_nulls: false)
    #   # =>
    #   # shape: (1,)
    #   # Series: '' [str]
    #   # [
    #   #         null
    #   # ]
    def join(delimiter = "-", ignore_nulls: true)
      super
    end
    alias_method :concat, :join

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

    # Decode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    # @param strict [Boolean]
    #   How to handle invalid inputs:
    #
    #   - `true`: An error will be thrown if unable to decode a value.
    #   - `false`: Unhandled values will be replaced with `nil`.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["666f6f", "626172", nil])
    #   s.str.decode("hex")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [binary]
    #   # [
    #   #         b"foo"
    #   #         b"bar"
    #   #         null
    #   # ]
    def decode(encoding, strict: false)
      super
    end

    # Encode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new(["foo", "bar", nil])
    #   s.str.encode("hex")
    #   # =>
    #   # shape: (3,)
    #   # Series: '' [str]
    #   # [
    #   #         "666f6f"
    #   #         "626172"
    #   #         null
    #   # ]
    def encode(encoding)
      super
    end

    # Extract the first match of json string with provided JSONPath expression.
    #
    # Throw errors if encounter invalid json strings.
    # All return value will be casted to Utf8 regardless of the original value.
    #
    # Documentation on JSONPath standard can be found
    # [here](https://goessner.net/articles/JsonPath/).
    #
    # @param json_path [String]
    #   A valid JSON path query string.
    #
    # @return [Series]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"json_val" => ['{"a":"1"}', nil, '{"a":2}', '{"a":2.1}', '{"a":true}']}
    #   )
    #   df.select(Polars.col("json_val").str.json_path_match("$.a"))[0.., 0]
    #   # =>
    #   # shape: (5,)
    #   # Series: 'json_val' [str]
    #   # [
    #   #         "1"
    #   #         null
    #   #         "2"
    #   #         "2.1"
    #   #         "true"
    #   # ]
    def json_path_match(json_path)
      super
    end

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
    #   # Series: 'foo' [list[str]]
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
    #   # │ null       ┆ null        │
    #   # │ c          ┆ null        │
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
    #   # │ null       ┆ null        │
    #   # │ foo-bar    ┆ null        │
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
    #
    # @example
    #   s = Polars::Series.new([" hello ", "\tworld"])
    #   s.str.strip_chars
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         "hello"
    #   #         "world"
    #   # ]
    def strip_chars(matches = nil)
      super
    end

    # Remove leading whitespace.
    #
    # @param matches [String, nil]
    #   An optional single character that should be trimmed
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([" hello ", "\tworld"])
    #   s.str.strip_chars_start
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         "hello "
    #   #         "world"
    #   # ]
    def strip_chars_start(matches = nil)
      super
    end
    alias_method :lstrip, :strip_chars_start

    # Remove trailing whitespace.
    #
    # @param matches [String, nil]
    #   An optional single character that should be trimmed
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([" hello ", "world\t"])
    #   s.str.strip_chars_end
    #   # =>
    #   # shape: (2,)
    #   # Series: '' [str]
    #   # [
    #   #         " hello"
    #   #         "world"
    #   # ]
    def strip_chars_end(matches = nil)
      super
    end
    alias_method :rstrip, :strip_chars_end

    # Fills the string with zeroes.
    #
    # Return a copy of the string left filled with ASCII '0' digits to make a string
    # of length width.
    #
    # A leading sign prefix ('+'/'-') is handled by inserting the padding after the
    # sign character rather than before. The original string is returned if width is
    # less than or equal to `s.length`.
    #
    # @param length [Integer]
    #   Fill the value up to this length.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new([-1, 123, 999999, nil])
    #   s.cast(Polars::String).str.zfill(4)
    #   # =>
    #   # shape: (4,)
    #   # Series: '' [str]
    #   # [
    #   #         "-001"
    #   #         "0123"
    #   #         "999999"
    #   #         null
    #   # ]
    def zfill(length)
      super
    end

    # Return the string left justified in a string of length `width`.
    #
    # Padding is done using the specified `fillchar`. The original string is
    # returned if `width` is less than or equal to `s.length`.
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
    #
    # @example
    #   s = Polars::Series.new("foo", ["CAT", "DOG"])
    #   s.str.to_lowercase
    #   # =>
    #   # shape: (2,)
    #   # Series: 'foo' [str]
    #   # [
    #   #         "cat"
    #   #         "dog"
    #   # ]
    def to_lowercase
      super
    end

    # Modify the strings to their uppercase equivalent.
    #
    # @return [Series]
    #
    # @example
    #   s = Polars::Series.new("foo", ["cat", "dog"])
    #   s.str.to_uppercase
    #   # =>
    #   # shape: (2,)
    #   # Series: 'foo' [str]
    #   # [
    #   #         "CAT"
    #   #         "DOG"
    #   # ]
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

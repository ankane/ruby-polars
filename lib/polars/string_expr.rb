module Polars
  # Namespace for string related expressions.
  class StringExpr
    # @private
    attr_accessor :_rbexpr

    # @private
    def initialize(expr)
      self._rbexpr = expr._rbexpr
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
    # @return [Expr]
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
      _validate_format_argument(format)
      Utils.wrap_expr(_rbexpr.str_to_date(format, strict, exact, cache))
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
    # @param ambiguous ['raise', 'earliest', 'latest', 'null']
    #   Determine how to deal with ambiguous datetimes:
    #
    #   - `'raise'` (default): raise
    #   - `'earliest'`: use the earliest datetime
    #   - `'latest'`: use the latest datetime
    #   - `'null'`: set to null
    #
    # @return [Expr]
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
      _validate_format_argument(format)
      unless ambiguous.is_a?(Expr)
        ambiguous = Polars.lit(ambiguous)
      end
      Utils.wrap_expr(
        _rbexpr.str_to_datetime(
          format,
          time_unit,
          time_zone,
          strict,
          exact,
          cache,
          ambiguous._rbexpr
        )
      )
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
    # @return [Expr]
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
      _validate_format_argument(format)
      Utils.wrap_expr(_rbexpr.str_to_time(format, strict, cache))
    end

    # Parse a Utf8 expression to a Date/Datetime/Time type.
    #
    # @param dtype [Object]
    #   The data type to convert into. Can be either Date, Datetime, or Time.
    # @param format [String]
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
    # @param ambiguous ['raise', 'earliest', 'latest', 'null']
    #   Determine how to deal with ambiguous datetimes:
    #
    #   - `'raise'` (default): raise
    #   - `'earliest'`: use the earliest datetime
    #   - `'latest'`: use the latest datetime
    #   - `'null'`: set to null
    #
    # @return [Expr]
    #
    # @note
    #   When parsing a Datetime the column precision will be inferred from
    #   the format string, if given, eg: "%F %T%.3f" => Datetime("ms"). If
    #   no fractional second component is found then the default is "us".
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
    #       "Sun Jul  8 00:34:60 2001",
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
    def strptime(
      dtype,
      format = nil,
      strict: true,
      exact: true,
      cache: true,
      ambiguous: "raise"
    )
      _validate_format_argument(format)

      if dtype == Date
        to_date(format, strict: strict, exact: exact, cache: cache)
      elsif dtype == Datetime || dtype.is_a?(Datetime)
        dtype = Datetime.new if dtype == Datetime
        time_unit = dtype.time_unit
        time_zone = dtype.time_zone
        to_datetime(
          format,
          time_unit: time_unit,
          time_zone: time_zone,
          strict: strict,
          exact: exact,
          cache: cache,
          ambiguous: ambiguous
        )
      elsif dtype == Time
        to_time(format, strict: strict, cache: cache)
      else
        raise ArgumentError, "dtype should be of type {Date, Datetime, Time}"
      end
    end

    # Convert a String column into a Decimal column.
    #
    # @param scale [Integer]
    #   Number of digits after the comma to use for the decimals.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "numbers": [
    #         "40.12",
    #         "3420.13",
    #         "120134.19",
    #         "3212.98",
    #         "12.90",
    #         "143.09",
    #         "143.9"
    #       ]
    #     }
    #   )
    #   df.with_columns(numbers_decimal: Polars.col("numbers").str.to_decimal(scale: 2))
    #   # =>
    #   # shape: (7, 2)
    #   # ┌───────────┬─────────────────┐
    #   # │ numbers   ┆ numbers_decimal │
    #   # │ ---       ┆ ---             │
    #   # │ str       ┆ decimal[38,2]   │
    #   # ╞═══════════╪═════════════════╡
    #   # │ 40.12     ┆ 40.12           │
    #   # │ 3420.13   ┆ 3420.13         │
    #   # │ 120134.19 ┆ 120134.19       │
    #   # │ 3212.98   ┆ 3212.98         │
    #   # │ 12.90     ┆ 12.90           │
    #   # │ 143.09    ┆ 143.09          │
    #   # │ 143.9     ┆ 143.90          │
    #   # └───────────┴─────────────────┘
    def to_decimal(scale:)
      Utils.wrap_expr(_rbexpr.str_to_decimal(scale))
    end

    # Get length of the strings as `:u32` (as number of bytes).
    #
    # @return [Expr]
    #
    # @note
    #   The returned lengths are equal to the number of bytes in the UTF8 string. If you
    #   need the length in terms of the number of characters, use `n_chars` instead.
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => ["Café", nil, "345", "東京"]}).with_columns(
    #     [
    #       Polars.col("s").str.len_bytes.alias("length"),
    #       Polars.col("s").str.len_chars.alias("nchars")
    #     ]
    #   )
    #   df
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬────────┬────────┐
    #   # │ s    ┆ length ┆ nchars │
    #   # │ ---  ┆ ---    ┆ ---    │
    #   # │ str  ┆ u32    ┆ u32    │
    #   # ╞══════╪════════╪════════╡
    #   # │ Café ┆ 5      ┆ 4      │
    #   # │ null ┆ null   ┆ null   │
    #   # │ 345  ┆ 3      ┆ 3      │
    #   # │ 東京 ┆ 6      ┆ 2      │
    #   # └──────┴────────┴────────┘
    def len_bytes
      Utils.wrap_expr(_rbexpr.str_len_bytes)
    end

    # Get length of the strings as `:u32` (as number of chars).
    #
    # @return [Expr]
    #
    # @note
    #   If you know that you are working with ASCII text, `lengths` will be
    #   equivalent, and faster (returns length in terms of the number of bytes).
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => ["Café", nil, "345", "東京"]}).with_columns(
    #     [
    #       Polars.col("s").str.len_bytes.alias("length"),
    #       Polars.col("s").str.len_chars.alias("nchars")
    #     ]
    #   )
    #   df
    #   # =>
    #   # shape: (4, 3)
    #   # ┌──────┬────────┬────────┐
    #   # │ s    ┆ length ┆ nchars │
    #   # │ ---  ┆ ---    ┆ ---    │
    #   # │ str  ┆ u32    ┆ u32    │
    #   # ╞══════╪════════╪════════╡
    #   # │ Café ┆ 5      ┆ 4      │
    #   # │ null ┆ null   ┆ null   │
    #   # │ 345  ┆ 3      ┆ 3      │
    #   # │ 東京 ┆ 6      ┆ 2      │
    #   # └──────┴────────┴────────┘
    def len_chars
      Utils.wrap_expr(_rbexpr.str_len_chars)
    end

    # Vertically concat the values in the Series to a single string value.
    #
    # @param delimiter [String]
    #   The delimiter to insert between consecutive string values.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, nil, 2]})
    #   df.select(Polars.col("foo").str.join("-"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ 1-2 │
    #   # └─────┘
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [1, nil, 2]})
    #   df.select(Polars.col("foo").str.join("-", ignore_nulls: false))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌──────┐
    #   # │ foo  │
    #   # │ ---  │
    #   # │ str  │
    #   # ╞══════╡
    #   # │ null │
    #   # └──────┘
    def join(delimiter = "", ignore_nulls: true)
      Utils.wrap_expr(_rbexpr.str_join(delimiter, ignore_nulls))
    end

    # Returns string values with all regular expression meta characters escaped.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"text" => ["abc", "def", nil, "abc(\\w+)"]})
    #   df.with_columns(Polars.col("text").str.escape_regex.alias("escaped"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────────┬──────────────┐
    #   # │ text     ┆ escaped      │
    #   # │ ---      ┆ ---          │
    #   # │ str      ┆ str          │
    #   # ╞══════════╪══════════════╡
    #   # │ abc      ┆ abc          │
    #   # │ def      ┆ def          │
    #   # │ null     ┆ null         │
    #   # │ abc(\w+) ┆ abc\(\\w\+\) │
    #   # └──────────┴──────────────┘
    def escape_regex
      Utils.wrap_expr(_rbexpr.str_escape_regex)
    end

    # Returns the Unicode normal form of the string values.
    #
    # This uses the forms described in Unicode Standard Annex 15: <https://www.unicode.org/reports/tr15/>.
    #
    # @param form ['NFC', 'NFKC', 'NFD', 'NFKD']
    #   Unicode form to use.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"text" => ["01²", "ＫＡＤＯＫＡＷＡ"]})
    #   new = df.with_columns(
    #     nfc: Polars.col("text").str.normalize("NFC"),
    #     nfkc: Polars.col("text").str.normalize("NFKC")
    #   )
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────────────────┬──────────────────┬──────────┐
    #   # │ text             ┆ nfc              ┆ nfkc     │
    #   # │ ---              ┆ ---              ┆ ---      │
    #   # │ str              ┆ str              ┆ str      │
    #   # ╞══════════════════╪══════════════════╪══════════╡
    #   # │ 01²              ┆ 01²              ┆ 012      │
    #   # │ ＫＡＤＯＫＡＷＡ ┆ ＫＡＤＯＫＡＷＡ ┆ KADOKAWA │
    #   # └──────────────────┴──────────────────┴──────────┘
    #
    # @example
    #   new.select(Polars.all.str.len_bytes)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌──────┬─────┬──────┐
    #   # │ text ┆ nfc ┆ nfkc │
    #   # │ ---  ┆ --- ┆ ---  │
    #   # │ u32  ┆ u32 ┆ u32  │
    #   # ╞══════╪═════╪══════╡
    #   # │ 4    ┆ 4   ┆ 3    │
    #   # │ 24   ┆ 24  ┆ 8    │
    #   # └──────┴─────┴──────┘
    def normalize(form = "NFC")
      Utils.wrap_expr(_rbexpr.str_normalize(form))
    end

    # Transform to uppercase variant.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["cat", "dog"]})
    #   df.select(Polars.col("foo").str.to_uppercase)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ CAT │
    #   # │ DOG │
    #   # └─────┘
    def to_uppercase
      Utils.wrap_expr(_rbexpr.str_to_uppercase)
    end

    # Transform to lowercase variant.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["CAT", "DOG"]})
    #   df.select(Polars.col("foo").str.to_lowercase)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ cat │
    #   # │ dog │
    #   # └─────┘
    def to_lowercase
      Utils.wrap_expr(_rbexpr.str_to_lowercase)
    end

    # Transform to titlecase variant.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"sing": ["welcome to my world", "THERE'S NO TURNING BACK"]}
    #   )
    #   df.with_columns(foo_title: Polars.col("sing").str.to_titlecase)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────────────────────────┬─────────────────────────┐
    #   # │ sing                    ┆ foo_title               │
    #   # │ ---                     ┆ ---                     │
    #   # │ str                     ┆ str                     │
    #   # ╞═════════════════════════╪═════════════════════════╡
    #   # │ welcome to my world     ┆ Welcome To My World     │
    #   # │ THERE'S NO TURNING BACK ┆ There's No Turning Back │
    #   # └─────────────────────────┴─────────────────────────┘
    def to_titlecase
      Utils.wrap_expr(_rbexpr.str_to_titlecase)
    end

    # Remove leading and trailing whitespace.
    #
    # @param characters [String, nil]
    #   An optional single character that should be trimmed.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [" lead", "trail ", " both "]})
    #   df.select(Polars.col("foo").str.strip_chars)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ foo   │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │ lead  │
    #   # │ trail │
    #   # │ both  │
    #   # └───────┘
    def strip_chars(characters = nil)
      characters = Utils.parse_into_expression(characters, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_strip_chars(characters))
    end

    # Remove leading whitespace.
    #
    # @param characters [String, nil]
    #   An optional single character that should be trimmed.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [" lead", "trail ", " both "]})
    #   df.select(Polars.col("foo").str.strip_chars_start)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌────────┐
    #   # │ foo    │
    #   # │ ---    │
    #   # │ str    │
    #   # ╞════════╡
    #   # │ lead   │
    #   # │ trail  │
    #   # │ both   │
    #   # └────────┘
    def strip_chars_start(characters = nil)
      characters = Utils.parse_into_expression(characters, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_strip_chars_start(characters))
    end

    # Remove trailing whitespace.
    #
    # @param characters [String, nil]
    #   An optional single character that should be trimmed.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => [" lead", "trail ", " both "]})
    #   df.select(Polars.col("foo").str.strip_chars_end)
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────┐
    #   # │ foo   │
    #   # │ ---   │
    #   # │ str   │
    #   # ╞═══════╡
    #   # │  lead │
    #   # │ trail │
    #   # │  both │
    #   # └───────┘
    def strip_chars_end(characters = nil)
      characters = Utils.parse_into_expression(characters, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_strip_chars_end(characters))
    end

    # Remove prefix.
    #
    # The prefix will be removed from the string exactly once, if found.
    #
    # @param prefix [String]
    #   The prefix to be removed.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => ["foobar", "foofoobar", "foo", "bar"]})
    #   df.with_columns(Polars.col("a").str.strip_prefix("foo").alias("stripped"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌───────────┬──────────┐
    #   # │ a         ┆ stripped │
    #   # │ ---       ┆ ---      │
    #   # │ str       ┆ str      │
    #   # ╞═══════════╪══════════╡
    #   # │ foobar    ┆ bar      │
    #   # │ foofoobar ┆ foobar   │
    #   # │ foo       ┆          │
    #   # │ bar       ┆ bar      │
    #   # └───────────┴──────────┘
    def strip_prefix(prefix)
      prefix = Utils.parse_into_expression(prefix, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_strip_prefix(prefix))
    end

    # Remove suffix.
    #
    # The suffix will be removed from the string exactly once, if found.
    #
    #
    # @param suffix [String]
    #   The suffix to be removed.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => ["foobar", "foobarbar", "foo", "bar"]})
    #   df.with_columns(Polars.col("a").str.strip_suffix("bar").alias("stripped"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌───────────┬──────────┐
    #   # │ a         ┆ stripped │
    #   # │ ---       ┆ ---      │
    #   # │ str       ┆ str      │
    #   # ╞═══════════╪══════════╡
    #   # │ foobar    ┆ foo      │
    #   # │ foobarbar ┆ foobar   │
    #   # │ foo       ┆ foo      │
    #   # │ bar       ┆          │
    #   # └───────────┴──────────┘
    def strip_suffix(suffix)
      suffix = Utils.parse_into_expression(suffix, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_strip_suffix(suffix))
    end

    # Pad the start of the string until it reaches the given length.
    #
    # @param length [Integer]
    #   Pad the string until it reaches this length. Strings with length equal to
    #   or greater than this value are returned as-is.
    # @param fill_char [String]
    #   The character to pad the string with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a": ["cow", "monkey", "hippopotamus", nil]})
    #   df.with_columns(padded: Polars.col("a").str.pad_start(8, "*"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────────────┬──────────────┐
    #   # │ a            ┆ padded       │
    #   # │ ---          ┆ ---          │
    #   # │ str          ┆ str          │
    #   # ╞══════════════╪══════════════╡
    #   # │ cow          ┆ *****cow     │
    #   # │ monkey       ┆ **monkey     │
    #   # │ hippopotamus ┆ hippopotamus │
    #   # │ null         ┆ null         │
    #   # └──────────────┴──────────────┘
    def pad_start(length, fill_char = " ")
      length = Utils.parse_into_expression(length)
      Utils.wrap_expr(_rbexpr.str_pad_start(length, fill_char))
    end

    # Pad the end of the string until it reaches the given length.
    #
    # @param length [Integer]
    #   Pad the string until it reaches this length. Strings with length equal to
    #   or greater than this value are returned as-is.
    # @param fill_char [String]
    #   The character to pad the string with.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a": ["cow", "monkey", "hippopotamus", nil]})
    #   df.with_columns(padded: Polars.col("a").str.pad_end(8, "*"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────────────┬──────────────┐
    #   # │ a            ┆ padded       │
    #   # │ ---          ┆ ---          │
    #   # │ str          ┆ str          │
    #   # ╞══════════════╪══════════════╡
    #   # │ cow          ┆ cow*****     │
    #   # │ monkey       ┆ monkey**     │
    #   # │ hippopotamus ┆ hippopotamus │
    #   # │ null         ┆ null         │
    #   # └──────────────┴──────────────┘
    def pad_end(length, fill_char = " ")
      length = Utils.parse_into_expression(length)
      Utils.wrap_expr(_rbexpr.str_pad_end(length, fill_char))
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
    # @param length [Integer]
    #   Fill the value up to this length
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => [-1, 123, 999999, nil]})
    #   df.with_columns(Polars.col("a").cast(Polars::String).str.zfill(4).alias("zfill"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌────────┬────────┐
    #   # │ a      ┆ zfill  │
    #   # │ ---    ┆ ---    │
    #   # │ i64    ┆ str    │
    #   # ╞════════╪════════╡
    #   # │ -1     ┆ -001   │
    #   # │ 123    ┆ 0123   │
    #   # │ 999999 ┆ 999999 │
    #   # │ null   ┆ null   │
    #   # └────────┴────────┘
    def zfill(length)
      length = Utils.parse_into_expression(length)
      Utils.wrap_expr(_rbexpr.str_zfill(length))
    end

    # Check if string contains a substring that matches a regex.
    #
    # @param pattern [String]
    #   A valid regex pattern.
    # @param literal [Boolean]
    #   Treat pattern as a literal string.
    # @param strict [Boolean]
    #   Raise an error if the underlying pattern is not a valid regex,
    #   otherwise mask out with a null value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => ["Crab", "cat and dog", "rab$bit", nil]})
    #   df.select(
    #     [
    #       Polars.col("a"),
    #       Polars.col("a").str.contains("cat|bit").alias("regex"),
    #       Polars.col("a").str.contains("rab$", literal: true).alias("literal")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────┬───────┬─────────┐
    #   # │ a           ┆ regex ┆ literal │
    #   # │ ---         ┆ ---   ┆ ---     │
    #   # │ str         ┆ bool  ┆ bool    │
    #   # ╞═════════════╪═══════╪═════════╡
    #   # │ Crab        ┆ false ┆ false   │
    #   # │ cat and dog ┆ true  ┆ false   │
    #   # │ rab$bit     ┆ true  ┆ true    │
    #   # │ null        ┆ null  ┆ null    │
    #   # └─────────────┴───────┴─────────┘
    def contains(pattern, literal: false, strict: true)
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_contains(pattern, literal, strict))
    end

    # Return the bytes offset of the first substring matching a pattern.
    #
    # If the pattern is not found, returns nil.
    #
    # @param pattern [String]
    #   A valid regular expression pattern, compatible with the [regex crate](https://docs.rs/regex/latest/regex/).
    # @param literal [Boolean]
    #   Treat `pattern` as a literal string, not as a regular expression.
    # @param strict [Boolean]
    #   Raise an error if the underlying pattern is not a valid regex,
    #   otherwise mask out with a null value.
    #
    # @return [Expr]
    #
    # @note
    #   To modify regular expression behaviour (such as case-sensitivity) with
    #   flags, use the inline `(?iLmsuxU)` syntax.
    #
    # @example Find the index of the first substring matching a regex or literal pattern:
    #   df = Polars::DataFrame.new(
    #     {
    #       "txt" => ["Crab", "Lobster", nil, "Crustacean"],
    #       "pat" => ["a[bc]", "b.t", "[aeiuo]", "(?i)A[BC]"]
    #     }
    #   )
    #   df.select(
    #     Polars.col("txt"),
    #     Polars.col("txt").str.find("a|e").alias("a|e (regex)"),
    #     Polars.col("txt").str.find("e", literal: true).alias("e (lit)"),
    #   )
    #   # =>
    #   # shape: (4, 3)
    #   # ┌────────────┬─────────────┬─────────┐
    #   # │ txt        ┆ a|e (regex) ┆ e (lit) │
    #   # │ ---        ┆ ---         ┆ ---     │
    #   # │ str        ┆ u32         ┆ u32     │
    #   # ╞════════════╪═════════════╪═════════╡
    #   # │ Crab       ┆ 2           ┆ null    │
    #   # │ Lobster    ┆ 5           ┆ 5       │
    #   # │ null       ┆ null        ┆ null    │
    #   # │ Crustacean ┆ 5           ┆ 7       │
    #   # └────────────┴─────────────┴─────────┘
    #
    # @example Match against a pattern found in another column or (expression):
    #   df.with_columns(Polars.col("txt").str.find(Polars.col("pat")).alias("find_pat"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌────────────┬───────────┬──────────┐
    #   # │ txt        ┆ pat       ┆ find_pat │
    #   # │ ---        ┆ ---       ┆ ---      │
    #   # │ str        ┆ str       ┆ u32      │
    #   # ╞════════════╪═══════════╪══════════╡
    #   # │ Crab       ┆ a[bc]     ┆ 2        │
    #   # │ Lobster    ┆ b.t       ┆ 2        │
    #   # │ null       ┆ [aeiuo]   ┆ null     │
    #   # │ Crustacean ┆ (?i)A[BC] ┆ 5        │
    #   # └────────────┴───────────┴──────────┘
    def find(pattern, literal: false, strict: true)
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_find(pattern, literal, strict))
    end

    # Check if string values end with a substring.
    #
    # @param suffix [String]
    #   Suffix substring.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"fruits" => ["apple", "mango", nil]})
    #   df.with_columns(
    #     Polars.col("fruits").str.ends_with("go").alias("has_suffix")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬────────────┐
    #   # │ fruits ┆ has_suffix │
    #   # │ ---    ┆ ---        │
    #   # │ str    ┆ bool       │
    #   # ╞════════╪════════════╡
    #   # │ apple  ┆ false      │
    #   # │ mango  ┆ true       │
    #   # │ null   ┆ null       │
    #   # └────────┴────────────┘
    #
    # @example Using `ends_with` as a filter condition:
    #   df.filter(Polars.col("fruits").str.ends_with("go"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌────────┐
    #   # │ fruits │
    #   # │ ---    │
    #   # │ str    │
    #   # ╞════════╡
    #   # │ mango  │
    #   # └────────┘
    def ends_with(suffix)
      suffix_rbexpr = Utils.parse_into_expression(suffix, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_ends_with(suffix_rbexpr))
    end

    # Check if string values start with a substring.
    #
    # @param prefix [String]
    #   Prefix substring.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"fruits" => ["apple", "mango", nil]})
    #   df.with_columns(
    #     Polars.col("fruits").str.starts_with("app").alias("has_prefix")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬────────────┐
    #   # │ fruits ┆ has_prefix │
    #   # │ ---    ┆ ---        │
    #   # │ str    ┆ bool       │
    #   # ╞════════╪════════════╡
    #   # │ apple  ┆ true       │
    #   # │ mango  ┆ false      │
    #   # │ null   ┆ null       │
    #   # └────────┴────────────┘
    #
    # @example Using `starts_with` as a filter condition:
    #   df.filter(Polars.col("fruits").str.starts_with("app"))
    #   # =>
    #   # shape: (1, 1)
    #   # ┌────────┐
    #   # │ fruits │
    #   # │ ---    │
    #   # │ str    │
    #   # ╞════════╡
    #   # │ apple  │
    #   # └────────┘
    def starts_with(prefix)
      prefix_rbexpr = Utils.parse_into_expression(prefix, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_starts_with(prefix_rbexpr))
    end

    # Parse string values as JSON.
    #
    # Throw errors if encounter invalid JSON strings.
    #
    # @param dtype [Object]
    #   The dtype to cast the extracted value to.
    # @param infer_schema_length [Integer]
    #   Deprecated and ignored.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"json" => ['{"a":1, "b": true}', nil, '{"a":2, "b": false}']}
    #   )
    #   dtype = Polars::Struct.new([Polars::Field.new("a", Polars::Int64), Polars::Field.new("b", Polars::Boolean)])
    #   df.with_columns(decoded: Polars.col("json").str.json_decode(dtype))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────┬───────────┐
    #   # │ json                ┆ decoded   │
    #   # │ ---                 ┆ ---       │
    #   # │ str                 ┆ struct[2] │
    #   # ╞═════════════════════╪═══════════╡
    #   # │ {"a":1, "b": true}  ┆ {1,true}  │
    #   # │ null                ┆ null      │
    #   # │ {"a":2, "b": false} ┆ {2,false} │
    #   # └─────────────────────┴───────────┘
    def json_decode(dtype, infer_schema_length: nil)
      if dtype.nil?
        msg = "`Expr.str.json_decode` needs an explicitly given `dtype` otherwise Polars is not able to determine the output type. If you want to eagerly infer datatype you can use `Series.str.json_decode`."
        raise TypeError, msg
      end

      dtype_expr = Utils.parse_into_datatype_expr(dtype)._rbdatatype_expr
      Utils.wrap_expr(_rbexpr.str_json_decode(dtype_expr))
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {"json_val" => ['{"a":"1"}', nil, '{"a":2}', '{"a":2.1}', '{"a":true}']}
    #   )
    #   df.select(Polars.col("json_val").str.json_path_match("$.a"))
    #   # =>
    #   # shape: (5, 1)
    #   # ┌──────────┐
    #   # │ json_val │
    #   # │ ---      │
    #   # │ str      │
    #   # ╞══════════╡
    #   # │ 1        │
    #   # │ null     │
    #   # │ 2        │
    #   # │ 2.1      │
    #   # │ true     │
    #   # └──────────┘
    def json_path_match(json_path)
      json_path = Utils.parse_into_expression(json_path, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_json_path_match(json_path))
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"encoded" => ["666f6f", "626172", nil]})
    #   df.select(Polars.col("encoded").str.decode("hex"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────┐
    #   # │ encoded │
    #   # │ ---     │
    #   # │ binary  │
    #   # ╞═════════╡
    #   # │ b"foo"  │
    #   # │ b"bar"  │
    #   # │ null    │
    #   # └─────────┘
    def decode(encoding, strict: true)
      if encoding == "hex"
        Utils.wrap_expr(_rbexpr.str_hex_decode(strict))
      elsif encoding == "base64"
        Utils.wrap_expr(_rbexpr.str_base64_decode(strict))
      else
        raise ArgumentError, "encoding must be one of {{'hex', 'base64'}}, got #{encoding}"
      end
    end

    # Encode a value using the provided encoding.
    #
    # @param encoding ["hex", "base64"]
    #   The encoding to use.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"strings" => ["foo", "bar", nil]})
    #   df.select(Polars.col("strings").str.encode("hex"))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────┐
    #   # │ strings │
    #   # │ ---     │
    #   # │ str     │
    #   # ╞═════════╡
    #   # │ 666f6f  │
    #   # │ 626172  │
    #   # │ null    │
    #   # └─────────┘
    def encode(encoding)
      if encoding == "hex"
        Utils.wrap_expr(_rbexpr.str_hex_encode)
      elsif encoding == "base64"
        Utils.wrap_expr(_rbexpr.str_base64_encode)
      else
        raise ArgumentError, "encoding must be one of {{'hex', 'base64'}}, got #{encoding}"
      end
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["123 bla 45 asd", "xyz 678 910t"]})
    #   df.select(
    #     [
    #       Polars.col("foo").str.extract('(\d+)')
    #     ]
    #   )
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
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_extract(pattern, group_index))
    end

    # Extracts all matches for the given regex pattern.
    #
    # Extracts each successive non-overlapping regex match in an individual string as
    # an array.
    #
    # @param pattern [String]
    #   A valid regex pattern
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["123 bla 45 asd", "xyz 678 910t"]})
    #   df.select(
    #     [
    #       Polars.col("foo").str.extract_all('(\d+)').alias("extracted_nrs")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────────────┐
    #   # │ extracted_nrs  │
    #   # │ ---            │
    #   # │ list[str]      │
    #   # ╞════════════════╡
    #   # │ ["123", "45"]  │
    #   # │ ["678", "910"] │
    #   # └────────────────┘
    def extract_all(pattern)
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_extract_all(pattern))
    end

    # Extract all capture groups for the given regex pattern.
    #
    # @param pattern [String]
    #   A valid regular expression pattern containing at least one capture group,
    #   compatible with the [regex crate](https://docs.rs/regex/latest/regex/).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "url": [
    #         "http://vote.com/ballon_dor?candidate=messi&ref=python",
    #         "http://vote.com/ballon_dor?candidate=weghorst&ref=polars",
    #         "http://vote.com/ballon_dor?error=404&ref=rust"
    #       ]
    #     }
    #   )
    #   pattern = /candidate=(?<candidate>\w+)&ref=(?<ref>\w+)/.to_s
    #   df.select(captures: Polars.col("url").str.extract_groups(pattern)).unnest(
    #     "captures"
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────┬────────┐
    #   # │ candidate ┆ ref    │
    #   # │ ---       ┆ ---    │
    #   # │ str       ┆ str    │
    #   # ╞═══════════╪════════╡
    #   # │ messi     ┆ python │
    #   # │ weghorst  ┆ polars │
    #   # │ null      ┆ null   │
    #   # └───────────┴────────┘
    #
    # @example Unnamed groups have their numerical position converted to a string:
    #   pattern = /candidate=(\w+)&ref=(\w+)/.to_s
    #   (
    #     df.with_columns(
    #       captures: Polars.col("url").str.extract_groups(pattern)
    #     ).with_columns(name: Polars.col("captures").struct["1"].str.to_uppercase)
    #   )
    #   # =>
    #   # shape: (3, 3)
    #   # ┌─────────────────────────────────┬───────────────────────┬──────────┐
    #   # │ url                             ┆ captures              ┆ name     │
    #   # │ ---                             ┆ ---                   ┆ ---      │
    #   # │ str                             ┆ struct[2]             ┆ str      │
    #   # ╞═════════════════════════════════╪═══════════════════════╪══════════╡
    #   # │ http://vote.com/ballon_dor?can… ┆ {"messi","python"}    ┆ MESSI    │
    #   # │ http://vote.com/ballon_dor?can… ┆ {"weghorst","polars"} ┆ WEGHORST │
    #   # │ http://vote.com/ballon_dor?err… ┆ {null,null}           ┆ null     │
    #   # └─────────────────────────────────┴───────────────────────┴──────────┘
    def extract_groups(pattern)
      Utils.wrap_expr(_rbexpr.str_extract_groups(pattern))
    end

    # Count all successive non-overlapping regex matches.
    #
    # @param pattern [String]
    #   A valid regex pattern
    # @param literal [Boolean]
    #   Treat `pattern` as a literal string, not as a regular expression.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"foo" => ["123 bla 45 asd", "xyz 678 910t"]})
    #   df.select(
    #     [
    #       Polars.col("foo").str.count_matches('\d').alias("count_digits")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 1)
    #   # ┌──────────────┐
    #   # │ count_digits │
    #   # │ ---          │
    #   # │ u32          │
    #   # ╞══════════════╡
    #   # │ 5            │
    #   # │ 6            │
    #   # └──────────────┘
    def count_matches(pattern, literal: false)
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_count_matches(pattern, literal))
    end

    # Split the string by a substring.
    #
    # @param by [String]
    #   Substring to split by.
    # @param inclusive [Boolean]
    #   If true, include the split character/string in the results.
    # @param literal [Boolean]
    #   Treat `by` as a literal string, not as a regular expression.
    # @param strict [Boolean]
    #   Raise an error if the underlying pattern is not a valid regex,
    #   otherwise mask out with a null value.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => ["foo bar", "foo-bar", "foo bar baz"]})
    #   df.select(Polars.col("s").str.split(" "))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌───────────────────────┐
    #   # │ s                     │
    #   # │ ---                   │
    #   # │ list[str]             │
    #   # ╞═══════════════════════╡
    #   # │ ["foo", "bar"]        │
    #   # │ ["foo-bar"]           │
    #   # │ ["foo", "bar", "baz"] │
    #   # └───────────────────────┘
    def split(by, inclusive: false, literal: true, strict: true)
      by_rbexpr = Utils.parse_into_expression(by, str_as_lit: true)

      if !literal
        if inclusive
          return Utils.wrap_expr(
            _rbexpr.str_split_regex_inclusive(by_rbexpr, strict)
          )
        end
        return Utils.wrap_expr(_rbexpr.str_split_regex(by_rbexpr, strict))
      end

      if inclusive
        return Utils.wrap_expr(_rbexpr.str_split_inclusive(by_rbexpr))
      end
      Utils.wrap_expr(_rbexpr.str_split(by_rbexpr))
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"x" => ["a_1", nil, "c", "d_4"]})
    #   df.select(
    #     [
    #       Polars.col("x").str.split_exact("_", 1).alias("fields")
    #     ]
    #   )
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────────────┐
    #   # │ fields      │
    #   # │ ---         │
    #   # │ struct[2]   │
    #   # ╞═════════════╡
    #   # │ {"a","1"}   │
    #   # │ {null,null} │
    #   # │ {"c",null}  │
    #   # │ {"d","4"}   │
    #   # └─────────────┘
    def split_exact(by, n, inclusive: false)
      by = Utils.parse_into_expression(by, str_as_lit: true)
      if inclusive
        Utils.wrap_expr(_rbexpr.str_split_exact_inclusive(by, n))
      else
        Utils.wrap_expr(_rbexpr.str_split_exact(by, n))
      end
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
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => ["foo bar", nil, "foo-bar", "foo bar baz"]})
    #   df.select(Polars.col("s").str.splitn(" ", 2).alias("fields"))
    #   # =>
    #   # shape: (4, 1)
    #   # ┌───────────────────┐
    #   # │ fields            │
    #   # │ ---               │
    #   # │ struct[2]         │
    #   # ╞═══════════════════╡
    #   # │ {"foo","bar"}     │
    #   # │ {null,null}       │
    #   # │ {"foo-bar",null}  │
    #   # │ {"foo","bar baz"} │
    #   # └───────────────────┘
    def splitn(by, n)
      by = Utils.parse_into_expression(by, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_splitn(by, n))
    end

    # Replace first matching regex/literal substring with a new string value.
    #
    # @param pattern [String]
    #   Regex pattern.
    # @param value [String]
    #   Replacement string.
    # @param literal [Boolean]
    #   Treat pattern as a literal string.
    # @param n [Integer]
    #   Number of matches to replace.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"id" => [1, 2], "text" => ["123abc", "abc456"]})
    #   df.with_columns(
    #     Polars.col("text").str.replace('abc\b', "ABC")
    #   )
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬────────┐
    #   # │ id  ┆ text   │
    #   # │ --- ┆ ---    │
    #   # │ i64 ┆ str    │
    #   # ╞═════╪════════╡
    #   # │ 1   ┆ 123ABC │
    #   # │ 2   ┆ abc456 │
    #   # └─────┴────────┘
    def replace(pattern, value, literal: false, n: 1)
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      value = Utils.parse_into_expression(value, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_replace_n(pattern, value, literal, n))
    end

    # Replace all matching regex/literal substrings with a new string value.
    #
    # @param pattern [String]
    #   Regex pattern.
    # @param value [String]
    #   Replacement string.
    # @param literal [Boolean]
    #   Treat pattern as a literal string.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"id" => [1, 2], "text" => ["abcabc", "123a123"]})
    #   df.with_columns(Polars.col("text").str.replace_all("a", "-"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────────┐
    #   # │ id  ┆ text    │
    #   # │ --- ┆ ---     │
    #   # │ i64 ┆ str     │
    #   # ╞═════╪═════════╡
    #   # │ 1   ┆ -bc-bc  │
    #   # │ 2   ┆ 123-123 │
    #   # └─────┴─────────┘
    def replace_all(pattern, value, literal: false)
      pattern = Utils.parse_into_expression(pattern, str_as_lit: true)
      value = Utils.parse_into_expression(value, str_as_lit: true)
      Utils.wrap_expr(_rbexpr.str_replace_all(pattern, value, literal))
    end

    # Returns string values in reversed order.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"text" => ["foo", "bar", "man\u0303ana"]})
    #   df.with_columns(Polars.col("text").str.reverse.alias("reversed"))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬──────────┐
    #   # │ text   ┆ reversed │
    #   # │ ---    ┆ ---      │
    #   # │ str    ┆ str      │
    #   # ╞════════╪══════════╡
    #   # │ foo    ┆ oof      │
    #   # │ bar    ┆ rab      │
    #   # │ mañana ┆ anañam   │
    #   # └────────┴──────────┘
    def reverse
      Utils.wrap_expr(_rbexpr.str_reverse)
    end

    # Create subslices of the string values of a Utf8 Series.
    #
    # @param offset [Integer]
    #   Start index. Negative indexing is supported.
    # @param length [Integer]
    #   Length of the slice. If set to `nil` (default), the slice is taken to the
    #   end of the string.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"s" => ["pear", nil, "papaya", "dragonfruit"]})
    #   df.with_columns(
    #     Polars.col("s").str.slice(-3).alias("s_sliced")
    #   )
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────┬──────────┐
    #   # │ s           ┆ s_sliced │
    #   # │ ---         ┆ ---      │
    #   # │ str         ┆ str      │
    #   # ╞═════════════╪══════════╡
    #   # │ pear        ┆ ear      │
    #   # │ null        ┆ null     │
    #   # │ papaya      ┆ aya      │
    #   # │ dragonfruit ┆ uit      │
    #   # └─────────────┴──────────┘
    def slice(offset, length = nil)
      offset = Utils.parse_into_expression(offset)
      length = Utils.parse_into_expression(length)
      Utils.wrap_expr(_rbexpr.str_slice(offset, length))
    end

    # Return the first n characters of each string in a String Series.
    #
    # @param n [Integer]
    #   Length of the slice (integer or expression). Negative indexing is supported;
    #   see note (2) below.
    #
    # @return [Expr]
    #
    # @note
    #   1) The `n` input is defined in terms of the number of characters in the (UTF8)
    #      string. A character is defined as a [Unicode scalar value](https://www.unicode.org/glossary/#unicode_scalar_value). A single
    #      character is represented by a single byte when working with ASCII text, and a
    #      maximum of 4 bytes otherwise.
    #
    #   2) When the `n` input is negative, `head` returns characters up to the `n`th
    #      from the end of the string. For example, if `n = -3`, then all characters
    #      except the last three are returned.
    #
    #   3) If the length of the string has fewer than `n` characters, the full string is
    #      returned.
    #
    # @example Return up to the first 5 characters:
    #   df = Polars::DataFrame.new({"s" => ["pear", nil, "papaya", "dragonfruit"]})
    #   df.with_columns(Polars.col("s").str.head(5).alias("s_head_5"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────┬──────────┐
    #   # │ s           ┆ s_head_5 │
    #   # │ ---         ┆ ---      │
    #   # │ str         ┆ str      │
    #   # ╞═════════════╪══════════╡
    #   # │ pear        ┆ pear     │
    #   # │ null        ┆ null     │
    #   # │ papaya      ┆ papay    │
    #   # │ dragonfruit ┆ drago    │
    #   # └─────────────┴──────────┘
    #
    # @example Return characters determined by column `n`:
    #   df = Polars::DataFrame.new(
    #     {
    #       "s" => ["pear", nil, "papaya", "dragonfruit"],
    #       "n" => [3, 4, -2, -5]
    #     }
    #   )
    #   df.with_columns(Polars.col("s").str.head("n").alias("s_head_n"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────┬─────┬──────────┐
    #   # │ s           ┆ n   ┆ s_head_n │
    #   # │ ---         ┆ --- ┆ ---      │
    #   # │ str         ┆ i64 ┆ str      │
    #   # ╞═════════════╪═════╪══════════╡
    #   # │ pear        ┆ 3   ┆ pea      │
    #   # │ null        ┆ 4   ┆ null     │
    #   # │ papaya      ┆ -2  ┆ papa     │
    #   # │ dragonfruit ┆ -5  ┆ dragon   │
    #   # └─────────────┴─────┴──────────┘
    def head(n)
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.str_head(n))
    end

    # Return the last n characters of each string in a String Series.
    #
    # @param n [Integer]
    #   Length of the slice (integer or expression). Negative indexing is supported;
    #   see note (2) below.
    #
    # @return [Expr]
    #
    # @note
    #   1) The `n` input is defined in terms of the number of characters in the (UTF8)
    #      string. A character is defined as a [Unicode scalar value](https://www.unicode.org/glossary/#unicode_scalar_value). A single
    #      character is represented by a single byte when working with ASCII text, and a
    #      maximum of 4 bytes otherwise.
    #
    #   2) When the `n` input is negative, `tail` returns characters starting from the
    #      `n`th from the beginning of the string. For example, if `n = -3`, then all
    #      characters except the first three are returned.
    #
    #   3) If the length of the string has fewer than `n` characters, the full string is
    #      returned.
    #
    # @example Return up to the last 5 characters:
    #   df = Polars::DataFrame.new({"s" => ["pear", nil, "papaya", "dragonfruit"]})
    #   df.with_columns(Polars.col("s").str.tail(5).alias("s_tail_5"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────────┬──────────┐
    #   # │ s           ┆ s_tail_5 │
    #   # │ ---         ┆ ---      │
    #   # │ str         ┆ str      │
    #   # ╞═════════════╪══════════╡
    #   # │ pear        ┆ pear     │
    #   # │ null        ┆ null     │
    #   # │ papaya      ┆ apaya    │
    #   # │ dragonfruit ┆ fruit    │
    #   # └─────────────┴──────────┘
    #
    # @example Return characters determined by column `n`:
    #   df = Polars::DataFrame.new(
    #     {
    #       "s" => ["pear", nil, "papaya", "dragonfruit"],
    #       "n" => [3, 4, -2, -5]
    #     }
    #   )
    #   df.with_columns(Polars.col("s").str.tail("n").alias("s_tail_n"))
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────────────┬─────┬──────────┐
    #   # │ s           ┆ n   ┆ s_tail_n │
    #   # │ ---         ┆ --- ┆ ---      │
    #   # │ str         ┆ i64 ┆ str      │
    #   # ╞═════════════╪═════╪══════════╡
    #   # │ pear        ┆ 3   ┆ ear      │
    #   # │ null        ┆ 4   ┆ null     │
    #   # │ papaya      ┆ -2  ┆ paya     │
    #   # │ dragonfruit ┆ -5  ┆ nfruit   │
    #   # └─────────────┴─────┴──────────┘
    def tail(n)
      n = Utils.parse_into_expression(n)
      Utils.wrap_expr(_rbexpr.str_tail(n))
    end

    # Convert an Utf8 column into an Int64 column with base radix.
    #
    # @param base [Integer]
    #   Positive integer which is the base of the string we are parsing.
    #   Default: 10.
    # @param strict [Boolean]
    #   Bool, default=true will raise any ParseError or overflow as ComputeError.
    #   false silently convert to Null.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new({"bin" => ["110", "101", "010", "invalid"]})
    #   df.with_columns(Polars.col("bin").str.to_integer(base: 2, strict: false).alias("parsed"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────────┬────────┐
    #   # │ bin     ┆ parsed │
    #   # │ ---     ┆ ---    │
    #   # │ str     ┆ i64    │
    #   # ╞═════════╪════════╡
    #   # │ 110     ┆ 6      │
    #   # │ 101     ┆ 5      │
    #   # │ 010     ┆ 2      │
    #   # │ invalid ┆ null   │
    #   # └─────────┴────────┘
    #
    # @example
    #   df = Polars::DataFrame.new({"hex" => ["fa1e", "ff00", "cafe", nil]})
    #   df.with_columns(Polars.col("hex").str.to_integer(base: 16, strict: true).alias("parsed"))
    #   # =>
    #   # shape: (4, 2)
    #   # ┌──────┬────────┐
    #   # │ hex  ┆ parsed │
    #   # │ ---  ┆ ---    │
    #   # │ str  ┆ i64    │
    #   # ╞══════╪════════╡
    #   # │ fa1e ┆ 64030  │
    #   # │ ff00 ┆ 65280  │
    #   # │ cafe ┆ 51966  │
    #   # │ null ┆ null   │
    #   # └──────┴────────┘
    def to_integer(base: 10, dtype: Int64, strict: true)
      base = Utils.parse_into_expression(base, str_as_lit: false)
      Utils.wrap_expr(_rbexpr.str_to_integer(base, dtype, strict))
    end

    # Use the aho-corasick algorithm to find matches.
    #
    # This version determines if any of the patterns find a match.
    #
    # @param patterns [String]
    #   String patterns to search.
    # @param ascii_case_insensitive [Boolean]
    #   Enable ASCII-aware case insensitive matching.
    #   When this option is enabled, searching will be performed without respect
    #   to case for ASCII letters (a-z and A-Z) only.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "lyrics": [
    #         "Everybody wants to rule the world",
    #         "Tell me what you want, what you really really want",
    #         "Can you feel the love tonight"
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("lyrics").str.contains_any(["you", "me"]).alias("contains_any")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────────────────┬──────────────┐
    #   # │ lyrics                          ┆ contains_any │
    #   # │ ---                             ┆ ---          │
    #   # │ str                             ┆ bool         │
    #   # ╞═════════════════════════════════╪══════════════╡
    #   # │ Everybody wants to rule the wo… ┆ false        │
    #   # │ Tell me what you want, what yo… ┆ true         │
    #   # │ Can you feel the love tonight   ┆ true         │
    #   # └─────────────────────────────────┴──────────────┘
    def contains_any(patterns, ascii_case_insensitive: false)
      patterns = Utils.parse_into_expression(patterns, str_as_lit: false)
      Utils.wrap_expr(
        _rbexpr.str_contains_any(patterns, ascii_case_insensitive)
      )
    end

    # Use the aho-corasick algorithm to replace many matches.
    #
    # @param patterns [Object]
    #   String patterns to search and replace.
    # @param replace_with [Object]
    #   Strings to replace where a pattern was a match.
    #   This can be broadcasted. So it supports many:one and many:many.
    # @param ascii_case_insensitive [Boolean]
    #   Enable ASCII-aware case insensitive matching.
    #   When this option is enabled, searching will be performed without respect
    #   to case for ASCII letters (a-z and A-Z) only.
    # @param leftmost [Boolean]
    #   Guarantees in case there are overlapping matches that the leftmost match
    #   is used. In case there are multiple candidates for the leftmost match
    #   the pattern which comes first in patterns is used.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "lyrics": [
    #         "Everybody wants to rule the world",
    #         "Tell me what you want, what you really really want",
    #         "Can you feel the love tonight"
    #       ]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.col("lyrics")
    #     .str.replace_many(
    #       ["me", "you", "they"],
    #       [""]
    #     )
    #     .alias("removes_pronouns")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────────────────┬─────────────────────────────────┐
    #   # │ lyrics                          ┆ removes_pronouns                │
    #   # │ ---                             ┆ ---                             │
    #   # │ str                             ┆ str                             │
    #   # ╞═════════════════════════════════╪═════════════════════════════════╡
    #   # │ Everybody wants to rule the wo… ┆ Everybody wants to rule the wo… │
    #   # │ Tell me what you want, what yo… ┆ Tell  what  want, what  really… │
    #   # │ Can you feel the love tonight   ┆ Can  feel the love tonight      │
    #   # └─────────────────────────────────┴─────────────────────────────────┘
    #
    # @example
    #   df.with_columns(
    #     Polars.col("lyrics")
    #     .str.replace_many(
    #       ["me", "you"],
    #       ["you", "me"]
    #     )
    #     .alias("confusing")
    #   )
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────────────────────────────────┬─────────────────────────────────┐
    #   # │ lyrics                          ┆ confusing                       │
    #   # │ ---                             ┆ ---                             │
    #   # │ str                             ┆ str                             │
    #   # ╞═════════════════════════════════╪═════════════════════════════════╡
    #   # │ Everybody wants to rule the wo… ┆ Everybody wants to rule the wo… │
    #   # │ Tell me what you want, what yo… ┆ Tell you what me want, what me… │
    #   # │ Can you feel the love tonight   ┆ Can me feel the love tonight    │
    #   # └─────────────────────────────────┴─────────────────────────────────┘
    def replace_many(
      patterns,
      replace_with = NO_DEFAULT,
      ascii_case_insensitive: false,
      leftmost: false
    )
      if replace_with == NO_DEFAULT
        if !patterns.is_a?(Hash)
          msg = "`replace_with` argument is required if `patterns` argument is not a Hash type"
          raise TypeError, msg
        end
        # Early return in case of an empty mapping.
        if patterns.empty?
          return Utils.wrap_expr(_rbexpr)
        end
        replace_with = patterns.values
        patterns = patterns.keys
      end

      patterns = Utils.parse_into_expression(patterns, str_as_lit: false)
      replace_with = Utils.parse_into_expression(replace_with, str_as_lit: true)
      Utils.wrap_expr(
        _rbexpr.str_replace_many(
          patterns, replace_with, ascii_case_insensitive, leftmost
        )
      )
    end

    # Use the Aho-Corasick algorithm to extract many matches.
    #
    # @param patterns [Object]
    #   String patterns to search.
    # @param ascii_case_insensitive [Boolean]
    #   Enable ASCII-aware case-insensitive matching.
    #   When this option is enabled, searching will be performed without respect
    #   to case for ASCII letters (a-z and A-Z) only.
    # @param overlapping [Boolean]
    #   Whether matches may overlap.
    # @param leftmost [Boolean]
    #   Guarantees in case there are overlapping matches that the leftmost match
    #   is used. In case there are multiple candidates for the leftmost match
    #   the pattern which comes first in patterns is used. May not be used
    #   together with overlapping: true.
    #
    # @return [Expr]
    #
    # @note
    #   This method supports matching on string literals only, and does not support
    #   regular expression matching.
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => ["discontent"]})
    #   patterns = ["winter", "disco", "onte", "discontent"]
    #   df.with_columns(
    #     Polars.col("values")
    #     .str.extract_many(patterns, overlapping: false)
    #     .alias("matches"),
    #     Polars.col("values")
    #     .str.extract_many(patterns, overlapping: true)
    #     .alias("matches_overlapping"),
    #   )
    #   # =>
    #   # shape: (1, 3)
    #   # ┌────────────┬───────────┬─────────────────────────────────┐
    #   # │ values     ┆ matches   ┆ matches_overlapping             │
    #   # │ ---        ┆ ---       ┆ ---                             │
    #   # │ str        ┆ list[str] ┆ list[str]                       │
    #   # ╞════════════╪═══════════╪═════════════════════════════════╡
    #   # │ discontent ┆ ["disco"] ┆ ["disco", "onte", "discontent"… │
    #   # └────────────┴───────────┴─────────────────────────────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "values" => ["discontent", "rhapsody"],
    #       "patterns" => [
    #         ["winter", "disco", "onte", "discontent"],
    #         ["rhap", "ody", "coalesce"]
    #       ]
    #     }
    #   )
    #   df.select(Polars.col("values").str.extract_many("patterns"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────┐
    #   # │ values          │
    #   # │ ---             │
    #   # │ list[str]       │
    #   # ╞═════════════════╡
    #   # │ ["disco"]       │
    #   # │ ["rhap", "ody"] │
    #   # └─────────────────┘
    def extract_many(
      patterns,
      ascii_case_insensitive: false,
      overlapping: false,
      leftmost: false
    )
      if overlapping && leftmost
        msg = "can not match overlapping patterns when leftmost == true"
        raise ArgumentError, msg
      end
      patterns = Utils.parse_into_expression(patterns, str_as_lit: false)
      Utils.wrap_expr(
        _rbexpr.str_extract_many(patterns, ascii_case_insensitive, overlapping, leftmost)
      )
    end

    # Use the Aho-Corasick algorithm to find many matches.
    #
    # The function will return the bytes offset of the start of each match.
    # The return type will be `List<UInt32>`
    #
    # @param patterns [Object]
    #   String patterns to search.
    # @param ascii_case_insensitive [Boolean]
    #   Enable ASCII-aware case-insensitive matching.
    #   When this option is enabled, searching will be performed without respect
    #   to case for ASCII letters (a-z and A-Z) only.
    # @param overlapping [Boolean]
    #   Whether matches may overlap.
    # @param leftmost [Boolean]
    #   Guarantees in case there are overlapping matches that the leftmost match
    #   is used. In case there are multiple candidates for the leftmost match
    #   the pattern which comes first in patterns is used. May not be used
    #   together with overlapping: true.
    #
    # @return [Expr]
    #
    # @note
    #   This method supports matching on string literals only, and does not support
    #   regular expression matching.
    #
    # @example
    #   df = Polars::DataFrame.new({"values" => ["discontent"]})
    #   patterns = ["winter", "disco", "onte", "discontent"]
    #   df.with_columns(
    #     Polars.col("values")
    #     .str.extract_many(patterns, overlapping: false)
    #     .alias("matches"),
    #     Polars.col("values")
    #     .str.extract_many(patterns, overlapping: true)
    #     .alias("matches_overlapping"),
    #   )
    #   # =>
    #   # shape: (1, 3)
    #   # ┌────────────┬───────────┬─────────────────────────────────┐
    #   # │ values     ┆ matches   ┆ matches_overlapping             │
    #   # │ ---        ┆ ---       ┆ ---                             │
    #   # │ str        ┆ list[str] ┆ list[str]                       │
    #   # ╞════════════╪═══════════╪═════════════════════════════════╡
    #   # │ discontent ┆ ["disco"] ┆ ["disco", "onte", "discontent"… │
    #   # └────────────┴───────────┴─────────────────────────────────┘
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "values" => ["discontent", "rhapsody"],
    #       "patterns" => [
    #         ["winter", "disco", "onte", "discontent"],
    #         ["rhap", "ody", "coalesce"]
    #       ]
    #     }
    #   )
    #   df.select(Polars.col("values").str.find_many("patterns"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ values    │
    #   # │ ---       │
    #   # │ list[u32] │
    #   # ╞═══════════╡
    #   # │ [0]       │
    #   # │ [0, 5]    │
    #   # └───────────┘
    def find_many(
      patterns,
      ascii_case_insensitive: false,
      overlapping: false,
      leftmost: false
    )
      if overlapping && leftmost
        msg = "can not match overlapping patterns when leftmost == true"
        raise ArgumentError, msg
      end
      patterns = Utils.parse_into_expression(patterns, str_as_lit: false)
      Utils.wrap_expr(
        _rbexpr.str_find_many(patterns, ascii_case_insensitive, overlapping, leftmost)
      )
    end

    private

    def _validate_format_argument(format)
      if !format.nil? && format.include?(".%f")
        message = (
          "Detected the pattern `.%f` in the chrono format string." +
          " This pattern should not be used to parse values after a decimal point." +
          " Use `%.f` instead." +
          " See the full specification: https://docs.rs/chrono/latest/chrono/format/strftime"
        )
        warn message
      end
    end
  end
end

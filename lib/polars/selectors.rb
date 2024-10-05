module Polars
  module Selectors
    # @private
    class SelectorProxy < Expr
      attr_accessor :_attrs
      attr_accessor :_repr_override

      def initialize(
        expr,
        name:,
        parameters: nil
      )
        self._rbexpr = expr._rbexpr
        self._attrs = {
          name: name,
          params: parameters
        }
      end

      def inspect
        if !_attrs
          as_expr.inspect
        elsif _repr_override
          _repr_override
        else
          selector_name = _attrs[:name]
          params = _attrs[:params] || {}
          set_ops = {"and" => "&", "or" => "|", "sub" => "-", "xor" => "^"}
          if set_ops.include?(selector_name)
            op = set_ops[selector_name]
            "(#{params.values.map(&:inspect).join(" #{op} ")})"
          else
            str_params = params.map { |k, v| k.start_with?("*") ? v.inspect[1..-2] : "#{k}=#{v.inspect}" }.join(", ")
            "Polars.cs.#{selector_name}(#{str_params})"
          end
        end
      end

      def ~
        if Utils.is_selector(self)
          inverted = Selectors.all - self
          inverted._repr_override = "~#{inspect}"
        else
          inverted = ~as_expr
        end
        inverted
      end

      def -(other)
        if Utils.is_selector(other)
          SelectorProxy.new(
            meta._as_selector.meta._selector_sub(other),
            parameters: {"self" => self, "other" => other},
            name: "sub"
          )
        else
          as_expr - other
        end
      end

      def &(other)
        if Utils.is_column(other)
          raise Todo
        end
        if Utils.is_selector(other)
          SelectorProxy.new(
            meta._as_selector.meta._selector_and(other),
            parameters: {"self" => self, "other" => other},
            name: "and"
          )
        else
          as_expr & other
        end
      end

      def |(other)
        if Utils.is_column(other)
          raise Todo
        end
        if Utils.is_selector(other)
          SelectorProxy.new(
            meta._as_selector.meta._selector_and(other),
            parameters: {"self" => self, "other" => other},
            name: "or"
          )
        else
          as_expr | other
        end
      end

      def ^(other)
        if Utils.is_column(other)
          raise Todo
        end
        if Utils.is_selector(other)
          SelectorProxy.new(
            meta._as_selector.meta._selector_and(other),
            parameters: {"self" => self, "other" => other},
            name: "xor"
          )
        else
          as_expr ^ other
        end
      end

      def as_expr
        Expr._from_rbexpr(_rbexpr)
      end
    end

    # @private
    def self._selector_proxy_(...)
      SelectorProxy.new(...)
    end

    # @private
    def self._re_string(string, escape: true)
      if string.is_a?(::String)
        rx = escape ? Utils.re_escape(string) : string
      else
        strings = []
        string.each do |st|
          if st.is_a?(Array)
            strings.concat(st)
          else
            strings << st
          end
        end
        rx = strings.map { |x| escape ? Utils.re_escape(x) : x }.join("|")
      end
      "(#{rx})"
    end

    # Select all columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dt" => [Date.new(1999, 12, 31), Date.new(2024, 1, 1)],
    #       "value" => [1_234_500, 5_000_555]
    #     },
    #     schema_overrides: {"value" => Polars::Int32}
    #   )
    #
    # @example Select all columns, casting them to string:
    #   df.select(Polars.cs.all.cast(Polars::String))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬─────────┐
    #   # │ dt         ┆ value   │
    #   # │ ---        ┆ ---     │
    #   # │ str        ┆ str     │
    #   # ╞════════════╪═════════╡
    #   # │ 1999-12-31 ┆ 1234500 │
    #   # │ 2024-01-01 ┆ 5000555 │
    #   # └────────────┴─────────┘
    #
    # @example Select all columns *except* for those matching the given dtypes:
    #   df.select(Polars.cs.all - Polars.cs.numeric)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────────┐
    #   # │ dt         │
    #   # │ ---        │
    #   # │ date       │
    #   # ╞════════════╡
    #   # │ 1999-12-31 │
    #   # │ 2024-01-01 │
    #   # └────────────┘
    def self.all
      _selector_proxy_(F.all, name: "all")
    end

    # Select all columns with alphabetic names (eg: only letters).
    #
    # @param ascii_only [Boolean]
    #   Indicate whether to consider only ASCII alphabetic characters, or the full
    #   Unicode range of valid letters (accented, idiographic, etc).
    # @param ignore_spaces [Boolean]
    #   Indicate whether to ignore the presence of spaces in column names; if so,
    #   only the other (non-space) characters are considered.
    #
    # @return [SelectorProxy]
    #
    # @note
    #   Matching column names cannot contain *any* non-alphabetic characters. Note
    #   that the definition of "alphabetic" consists of all valid Unicode alphabetic
    #   characters (`\p{Alphabetic}`) by default; this can be changed by setting
    #   `ascii_only: true`.
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "no1" => [100, 200, 300],
    #       "café" => ["espresso", "latte", "mocha"],
    #       "t or f" => [true, false, nil],
    #       "hmm" => ["aaa", "bbb", "ccc"],
    #       "都市" => ["東京", "大阪", "京都"]
    #     }
    #   )
    #
    # @example Select columns with alphabetic names; note that accented characters and kanji are recognised as alphabetic here:
    #   df.select(Polars.cs.alpha)
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────────┬─────┬──────┐
    #   # │ café     ┆ hmm ┆ 都市 │
    #   # │ ---      ┆ --- ┆ ---  │
    #   # │ str      ┆ str ┆ str  │
    #   # ╞══════════╪═════╪══════╡
    #   # │ espresso ┆ aaa ┆ 東京 │
    #   # │ latte    ┆ bbb ┆ 大阪 │
    #   # │ mocha    ┆ ccc ┆ 京都 │
    #   # └──────────┴─────┴──────┘
    #
    # @example Constrain the definition of "alphabetic" to ASCII characters only:
    #   df.select(Polars.cs.alpha(ascii_only: true))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ hmm │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ aaa │
    #   # │ bbb │
    #   # │ ccc │
    #   # └─────┘
    #
    # @example
    #   df.select(Polars.cs.alpha(ascii_only: true, ignore_spaces: true))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌────────┬─────┐
    #   # │ t or f ┆ hmm │
    #   # │ ---    ┆ --- │
    #   # │ bool   ┆ str │
    #   # ╞════════╪═════╡
    #   # │ true   ┆ aaa │
    #   # │ false  ┆ bbb │
    #   # │ null   ┆ ccc │
    #   # └────────┴─────┘
    #
    # @example Select all columns *except* for those with alphabetic names:
    #   df.select(~Polars.cs.alpha)
    #   # =>
    #   # shape: (3, 2)
    #   # ┌─────┬────────┐
    #   # │ no1 ┆ t or f │
    #   # │ --- ┆ ---    │
    #   # │ i64 ┆ bool   │
    #   # ╞═════╪════════╡
    #   # │ 100 ┆ true   │
    #   # │ 200 ┆ false  │
    #   # │ 300 ┆ null   │
    #   # └─────┴────────┘
    #
    # @example
    #   df.select(~Polars.cs.alpha(ignore_spaces: true))
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────┐
    #   # │ no1 │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 100 │
    #   # │ 200 │
    #   # │ 300 │
    #   # └─────┘
    def self.alpha(ascii_only: false, ignore_spaces: false)
      # note that we need to supply a pattern compatible with the *rust* regex crate
      re_alpha = ascii_only ? "a-zA-Z" : "\\p{Alphabetic}"
      re_space = ignore_spaces ? " " : ""
      _selector_proxy_(
        F.col("^[#{re_alpha}#{re_space}]+$"),
        name: "alpha",
        parameters: {"ascii_only" => ascii_only, "ignore_spaces" => ignore_spaces},
      )
    end

    # TODO
    # def alphanumeric
    # end

    # Select all binary columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new({"a" => ["hello".b], "b" => ["world"], "c" => ["!".b], "d" => [":)"]})
    #   # =>
    #   # shape: (1, 4)
    #   # ┌──────────┬───────┬────────┬─────┐
    #   # │ a        ┆ b     ┆ c      ┆ d   │
    #   # │ ---      ┆ ---   ┆ ---    ┆ --- │
    #   # │ binary   ┆ str   ┆ binary ┆ str │
    #   # ╞══════════╪═══════╪════════╪═════╡
    #   # │ b"hello" ┆ world ┆ b"!"   ┆ :)  │
    #   # └──────────┴───────┴────────┴─────┘
    #
    # @example Select binary columns and export as a dict:
    #   df.select(Polars.cs.binary).to_h(as_series: false)
    #   # => {"a"=>["hello"], "c"=>["!"]}
    #
    # @example Select all columns *except* for those that are binary:
    #   df.select(~Polars.cs.binary).to_h(as_series: false)
    #   # => {"b"=>["world"], "d"=>[":)"]}
    def self.binary
      _selector_proxy_(F.col(Binary), name: "binary")
    end

    # Select all boolean columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new({"n" => 1..4}).with_columns(n_even: Polars.col("n") % 2 == 0)
    #   # =>
    #   # shape: (4, 2)
    #   # ┌─────┬────────┐
    #   # │ n   ┆ n_even │
    #   # │ --- ┆ ---    │
    #   # │ i64 ┆ bool   │
    #   # ╞═════╪════════╡
    #   # │ 1   ┆ false  │
    #   # │ 2   ┆ true   │
    #   # │ 3   ┆ false  │
    #   # │ 4   ┆ true   │
    #   # └─────┴────────┘
    #
    # @example Select and invert boolean columns:
    #   df.with_columns(is_odd: Polars.cs.boolean.not_)
    #   # =>
    #   # shape: (4, 3)
    #   # ┌─────┬────────┬────────┐
    #   # │ n   ┆ n_even ┆ is_odd │
    #   # │ --- ┆ ---    ┆ ---    │
    #   # │ i64 ┆ bool   ┆ bool   │
    #   # ╞═════╪════════╪════════╡
    #   # │ 1   ┆ false  ┆ true   │
    #   # │ 2   ┆ true   ┆ false  │
    #   # │ 3   ┆ false  ┆ true   │
    #   # │ 4   ┆ true   ┆ false  │
    #   # └─────┴────────┴────────┘
    #
    # @example Select all columns *except* for those that are boolean:
    #   df.select(~Polars.cs.boolean)
    #   # =>
    #   # shape: (4, 1)
    #   # ┌─────┐
    #   # │ n   │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 1   │
    #   # │ 2   │
    #   # │ 3   │
    #   # │ 4   │
    #   # └─────┘
    def self.boolean
      _selector_proxy_(F.col(Boolean), name: "boolean")
    end

    # TODO
    # def by_dtype
    # end

    # TODO
    # def by_index
    # end

    # TODO
    # def by_name
    # end

    # Select all categorical columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["xx", "yy"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5]
    #     },
    #     schema_overrides: {"foo" => Polars::Categorical}
    #   )
    #
    # @example Select all categorical columns:
    #   df.select(Polars.cs.categorical)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ cat │
    #   # ╞═════╡
    #   # │ xx  │
    #   # │ yy  │
    #   # └─────┘
    #
    # @example Select all columns *except* for those that are categorical:
    #   df.select(~Polars.cs.categorical)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ bar ┆ baz │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 123 ┆ 2.0 │
    #   # │ 456 ┆ 5.5 │
    #   # └─────┴─────┘
    def self.categorical
      _selector_proxy_(F.col(Categorical), name: "categorical")
    end

    # TODO
    # def contains
    # end

    # Select all date columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "dtm" => [DateTime.new(2001, 5, 7, 10, 25), DateTime.new(2031, 12, 31, 0, 30)],
    #       "dt" => [Date.new(1999, 12, 31), Date.new(2024, 8, 9)]
    #     }
    #   )
    #
    # @example Select all date columns:
    #   df.select(Polars.cs.date)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌────────────┐
    #   # │ dt         │
    #   # │ ---        │
    #   # │ date       │
    #   # ╞════════════╡
    #   # │ 1999-12-31 │
    #   # │ 2024-08-09 │
    #   # └────────────┘
    #
    # @example Select all columns *except* for those that are dates:
    #   df.select(~Polars.cs.date)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ dtm                 │
    #   # │ ---                 │
    #   # │ datetime[ns]        │
    #   # ╞═════════════════════╡
    #   # │ 2001-05-07 10:25:00 │
    #   # │ 2031-12-31 00:30:00 │
    #   # └─────────────────────┘
    def self.date
      _selector_proxy_(F.col(Date), name: "date")
    end

    # TODO
    # def datetime
    # end

    # Select all decimal columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [BigDecimal("123"), BigDecimal("456")],
    #       "baz" => [BigDecimal("2.0005"), BigDecimal("-50.5555")],
    #     },
    #     schema_overrides: {"baz" => Polars::Decimal.new(10, 5)}
    #   )
    #
    # @example Select all decimal columns:
    #   df.select(Polars.cs.decimal)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌──────────────┬───────────────┐
    #   # │ bar          ┆ baz           │
    #   # │ ---          ┆ ---           │
    #   # │ decimal[*,0] ┆ decimal[10,5] │
    #   # ╞══════════════╪═══════════════╡
    #   # │ 123          ┆ 2.00050       │
    #   # │ 456          ┆ -50.55550     │
    #   # └──────────────┴───────────────┘
    #
    # @example Select all columns *except* the decimal ones:
    #
    #   df.select(~Polars.cs.decimal)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ x   │
    #   # │ y   │
    #   # └─────┘
    def self.decimal
      # TODO: allow explicit selection by scale/precision?
      _selector_proxy_(F.col(Decimal), name: "decimal")
    end

    # Select columns that end with the given substring(s).
    #
    # @param suffix [Object]
    #   Substring(s) that matching column names should end with.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [false, true]
    #     }
    #   )
    #
    # @example Select columns that end with the substring 'z':
    #   df.select(Polars.cs.ends_with("z"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ baz │
    #   # │ --- │
    #   # │ f64 │
    #   # ╞═════╡
    #   # │ 2.0 │
    #   # │ 5.5 │
    #   # └─────┘
    #
    # @example Select columns that end with *either* the letter 'z' or 'r':
    #   df.select(Polars.cs.ends_with("z", "r"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ bar ┆ baz │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 123 ┆ 2.0 │
    #   # │ 456 ┆ 5.5 │
    #   # └─────┴─────┘
    #
    # @example Select all columns *except* for those that end with the substring 'z':
    #   df.select(~Polars.cs.ends_with("z"))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬───────┐
    #   # │ foo ┆ bar ┆ zap   │
    #   # │ --- ┆ --- ┆ ---   │
    #   # │ str ┆ i64 ┆ bool  │
    #   # ╞═════╪═════╪═══════╡
    #   # │ x   ┆ 123 ┆ false │
    #   # │ y   ┆ 456 ┆ true  │
    #   # └─────┴─────┴───────┘
    def self.ends_with(*suffix)
      escaped_suffix = _re_string(suffix)
      raw_params = "^.*#{escaped_suffix}$"

      _selector_proxy_(
        F.col(raw_params),
        name: "ends_with",
        parameters: {"*suffix" => escaped_suffix},
      )
    end

    # Select the first column in the current scope.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [0, 1]
    #     }
    #   )
    #
    # @example Select the first column:
    #   df.select(Polars.cs.first)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ x   │
    #   # │ y   │
    #   # └─────┘
    #
    # @example Select everything *except* for the first column:
    #   df.select(~Polars.cs.first)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ bar ┆ baz ┆ zap │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i64 ┆ f64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 123 ┆ 2.0 ┆ 0   │
    #   # │ 456 ┆ 5.5 ┆ 1   │
    #   # └─────┴─────┴─────┘
    def self.first
      _selector_proxy_(F.first, name: "first")
    end

    # Select all float columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [0.0, 1.0]
    #     },
    #     schema_overrides: {"baz" => Polars::Float32, "zap" => Polars::Float64}
    #   )
    #
    # @example Select all float columns:
    #   df.select(Polars.cs.float)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ baz ┆ zap │
    #   # │ --- ┆ --- │
    #   # │ f32 ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ 2.0 ┆ 0.0 │
    #   # │ 5.5 ┆ 1.0 │
    #   # └─────┴─────┘
    #
    # @example Select all columns *except* for those that are float:
    #   df.select(~Polars.cs.float)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ bar │
    #   # │ --- ┆ --- │
    #   # │ str ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ x   ┆ 123 │
    #   # │ y   ┆ 456 │
    #   # └─────┴─────┘
    def self.float
      _selector_proxy_(F.col(FLOAT_DTYPES), name: "float")
    end

    # Select all integer columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [0, 1]
    #     }
    #   )
    #
    # @example Select all integer columns:
    #   df.select(Polars.cs.integer)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ bar ┆ zap │
    #   # │ --- ┆ --- │
    #   # │ i64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 123 ┆ 0   │
    #   # │ 456 ┆ 1   │
    #   # └─────┴─────┘
    #
    # @example Select all columns *except* for those that are integer:
    #   df.select(~Polars.cs.integer)
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ baz │
    #   # │ --- ┆ --- │
    #   # │ str ┆ f64 │
    #   # ╞═════╪═════╡
    #   # │ x   ┆ 2.0 │
    #   # │ y   ┆ 5.5 │
    #   # └─────┴─────┘
    def self.integer
      _selector_proxy_(F.col(INTEGER_DTYPES), name: "integer")
    end

    # Select the last column in the current scope.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [0, 1]
    #     }
    #   )
    #
    # @example Select the last column:
    #   df.select(Polars.cs.last)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ zap │
    #   # │ --- │
    #   # │ i64 │
    #   # ╞═════╡
    #   # │ 0   │
    #   # │ 1   │
    #   # └─────┘
    #
    # @example Select everything *except* for the last column:
    #   df.select(~Polars.cs.last)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ foo ┆ bar ┆ baz │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ str ┆ i64 ┆ f64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ x   ┆ 123 ┆ 2.0 │
    #   # │ y   ┆ 456 ┆ 5.5 │
    #   # └─────┴─────┴─────┘
    def self.last
      _selector_proxy_(F.last, name: "last")
    end

    # Select all numeric columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => ["x", "y"],
    #       "bar" => [123, 456],
    #       "baz" => [2.0, 5.5],
    #       "zap" => [0, 0]
    #     },
    #     schema_overrides: {"bar" => Polars::Int16, "baz" => Polars::Float32, "zap" => Polars::UInt8},
    #   )
    #
    # @example Match all numeric columns:
    #   df.select(Polars.cs.numeric)
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ bar ┆ baz ┆ zap │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ i16 ┆ f32 ┆ u8  │
    #   # ╞═════╪═════╪═════╡
    #   # │ 123 ┆ 2.0 ┆ 0   │
    #   # │ 456 ┆ 5.5 ┆ 0   │
    #   # └─────┴─────┴─────┘
    #
    # @example Match all columns *except* for those that are numeric:
    #   df.select(~Polars.cs.numeric)
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────┐
    #   # │ foo │
    #   # │ --- │
    #   # │ str │
    #   # ╞═════╡
    #   # │ x   │
    #   # │ y   │
    #   # └─────┘
    def self.numeric
      _selector_proxy_(F.col(NUMERIC_DTYPES), name: "numeric")
    end

    # Select columns that start with the given substring(s).
    #
    # @param prefix [Object]
    #   Substring(s) that matching column names should start with.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "foo" => [1.0, 2.0],
    #       "bar" => [3.0, 4.0],
    #       "baz" => [5, 6],
    #       "zap" => [7, 8]
    #     }
    #   )
    #
    # @example Match columns starting with a 'b':
    #   df.select(Polars.cs.starts_with("b"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ bar ┆ baz │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 3.0 ┆ 5   │
    #   # │ 4.0 ┆ 6   │
    #   # └─────┴─────┘
    #
    # @example Match columns starting with *either* the letter 'b' or 'z':
    #   df.select(Polars.cs.starts_with("b", "z"))
    #   # =>
    #   # shape: (2, 3)
    #   # ┌─────┬─────┬─────┐
    #   # │ bar ┆ baz ┆ zap │
    #   # │ --- ┆ --- ┆ --- │
    #   # │ f64 ┆ i64 ┆ i64 │
    #   # ╞═════╪═════╪═════╡
    #   # │ 3.0 ┆ 5   ┆ 7   │
    #   # │ 4.0 ┆ 6   ┆ 8   │
    #   # └─────┴─────┴─────┘
    #
    # @example Match all columns *except* for those starting with 'b':
    #   df.select(~Polars.cs.starts_with("b"))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────┬─────┐
    #   # │ foo ┆ zap │
    #   # │ --- ┆ --- │
    #   # │ f64 ┆ i64 │
    #   # ╞═════╪═════╡
    #   # │ 1.0 ┆ 7   │
    #   # │ 2.0 ┆ 8   │
    #   # └─────┴─────┘
    def self.starts_with(*prefix)
      escaped_prefix = _re_string(prefix)
      raw_params = "^#{escaped_prefix}.*$"

      _selector_proxy_(
        F.col(raw_params),
        name: "starts_with",
        parameters: {"*prefix" => prefix}
      )
    end

    # Select all String (and, optionally, Categorical) string columns.
    #
    # @return [SelectorProxy]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "w" => ["xx", "yy", "xx", "yy", "xx"],
    #       "x" => [1, 2, 1, 4, -2],
    #       "y" => [3.0, 4.5, 1.0, 2.5, -2.0],
    #       "z" => ["a", "b", "a", "b", "b"]
    #     },
    #   ).with_columns(
    #     z: Polars.col("z").cast(Polars::Categorical.new("lexical")),
    #   )
    #
    # @example Group by all string columns, sum the numeric columns, then sort by the string cols:
    # >>> df.group_by(Polars.cs.string).agg(Polars.cs.numeric.sum).sort(Polars.cs.string)
    # shape: (2, 3)
    # ┌─────┬─────┬─────┐
    # │ w   ┆ x   ┆ y   │
    # │ --- ┆ --- ┆ --- │
    # │ str ┆ i64 ┆ f64 │
    # ╞═════╪═════╪═════╡
    # │ xx  ┆ 0   ┆ 2.0 │
    # │ yy  ┆ 6   ┆ 7.0 │
    # └─────┴─────┴─────┘
    #
    # @example Group by all string *and* categorical columns:
    #   df.group_by(Polars.cs.string(include_categorical: true)).agg(Polars.cs.numeric.sum).sort(
    #     Polars.cs.string(include_categorical: true)
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬─────┬─────┬──────┐
    #   # │ w   ┆ z   ┆ x   ┆ y    │
    #   # │ --- ┆ --- ┆ --- ┆ ---  │
    #   # │ str ┆ cat ┆ i64 ┆ f64  │
    #   # ╞═════╪═════╪═════╪══════╡
    #   # │ xx  ┆ a   ┆ 2   ┆ 4.0  │
    #   # │ xx  ┆ b   ┆ -2  ┆ -2.0 │
    #   # │ yy  ┆ b   ┆ 6   ┆ 7.0  │
    #   # └─────┴─────┴─────┴──────┘
    def self.string(include_categorical: false)
      string_dtypes = [String]
      if include_categorical
        string_dtypes << Categorical
      end

      _selector_proxy_(
        F.col(string_dtypes),
        name: "string",
        parameters: {"include_categorical" => include_categorical},
      )
    end
  end

  def self.cs
    Polars::Selectors
  end
end

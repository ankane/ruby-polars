module Polars
  module Functions
    # Create a Polars literal expression of type Datetime.
    #
    # @param year [Object]
    #   Column or literal.
    # @param month [Object]
    #   Column or literal, ranging from 1-12.
    # @param day [Object]
    #   Column or literal, ranging from 1-31.
    # @param hour [Object]
    #   Column or literal, ranging from 0-23.
    # @param minute [Object]
    #   Column or literal, ranging from 0-59.
    # @param second [Object]
    #   Column or literal, ranging from 0-59.
    # @param microsecond [Object]
    #   Column or literal, ranging from 0-999999.
    # @param time_unit ['us', 'ms', 'ns']
    #   Time unit of the resulting expression.
    # @param time_zone [Object]
    #   Time zone of the resulting expression.
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
    #   df = Polars::DataFrame.new(
    #     {
    #       "month" => [1, 2, 3],
    #       "day" => [4, 5, 6],
    #       "hour" => [12, 13, 14],
    #       "minute" => [15, 30, 45]
    #     }
    #   )
    #   df.with_columns(
    #     Polars.datetime(
    #       2024,
    #       Polars.col("month"),
    #       Polars.col("day"),
    #       Polars.col("hour"),
    #       Polars.col("minute"),
    #       time_zone: "Australia/Sydney"
    #     )
    #   )
    #   # =>
    #   # shape: (3, 5)
    #   # ┌───────┬─────┬──────┬────────┬────────────────────────────────┐
    #   # │ month ┆ day ┆ hour ┆ minute ┆ datetime                       │
    #   # │ ---   ┆ --- ┆ ---  ┆ ---    ┆ ---                            │
    #   # │ i64   ┆ i64 ┆ i64  ┆ i64    ┆ datetime[μs, Australia/Sydney] │
    #   # ╞═══════╪═════╪══════╪════════╪════════════════════════════════╡
    #   # │ 1     ┆ 4   ┆ 12   ┆ 15     ┆ 2024-01-04 12:15:00 AEDT       │
    #   # │ 2     ┆ 5   ┆ 13   ┆ 30     ┆ 2024-02-05 13:30:00 AEDT       │
    #   # │ 3     ┆ 6   ┆ 14   ┆ 45     ┆ 2024-03-06 14:45:00 AEDT       │
    #   # └───────┴─────┴──────┴────────┴────────────────────────────────┘
    #
    # @example We can also use `Polars.datetime` for filtering:
    #   df = Polars::DataFrame.new(
    #     {
    #       "start" => [
    #         DateTime.new(2024, 1, 1, 0, 0, 0),
    #         DateTime.new(2024, 1, 1, 0, 0, 0),
    #         DateTime.new(2024, 1, 1, 0, 0, 0)
    #       ],
    #       "end" => [
    #         DateTime.new(2024, 5, 1, 20, 15, 10),
    #         DateTime.new(2024, 7, 1, 21, 25, 20),
    #         DateTime.new(2024, 9, 1, 22, 35, 30)
    #       ]
    #     }
    #   )
    #   df.filter(Polars.col("end") > Polars.datetime(2024, 6, 1))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌─────────────────────┬─────────────────────┐
    #   # │ start               ┆ end                 │
    #   # │ ---                 ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╡
    #   # │ 2024-01-01 00:00:00 ┆ 2024-07-01 21:25:20 │
    #   # │ 2024-01-01 00:00:00 ┆ 2024-09-01 22:35:30 │
    #   # └─────────────────────┴─────────────────────┘
    def datetime(
      year,
      month,
      day,
      hour = nil,
      minute = nil,
      second = nil,
      microsecond = nil,
      time_unit: "us",
      time_zone: nil,
      ambiguous: "raise"
    )
      ambiguous_expr = Utils.parse_into_expression(ambiguous, str_as_lit: true)
      year_expr = Utils.parse_into_expression(year)
      month_expr = Utils.parse_into_expression(month)
      day_expr = Utils.parse_into_expression(day)

      hour_expr = !hour.nil? ? Utils.parse_into_expression(hour) : nil
      minute_expr = !minute.nil? ? Utils.parse_into_expression(minute) : nil
      second_expr = !second.nil? ? Utils.parse_into_expression(second) : nil
      microsecond_expr = (
        !microsecond.nil? ? Utils.parse_into_expression(microsecond) : nil
      )

      Utils.wrap_expr(
        Plr.datetime(
          year_expr,
          month_expr,
          day_expr,
          hour_expr,
          minute_expr,
          second_expr,
          microsecond_expr,
          time_unit,
          time_zone,
          ambiguous_expr
        )
      )
    end

    # Create a Polars literal expression of type Date.
    #
    # @param year [Object]
    #   column or literal.
    # @param month [Object]
    #   column or literal, ranging from 1-12.
    # @param day [Object]
    #   column or literal, ranging from 1-31.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "month" => [1, 2, 3],
    #       "day" => [4, 5, 6]
    #     }
    #   )
    #   df.with_columns(Polars.date(2024, Polars.col("month"), Polars.col("day")))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────┬─────┬────────────┐
    #   # │ month ┆ day ┆ date       │
    #   # │ ---   ┆ --- ┆ ---        │
    #   # │ i64   ┆ i64 ┆ date       │
    #   # ╞═══════╪═════╪════════════╡
    #   # │ 1     ┆ 4   ┆ 2024-01-04 │
    #   # │ 2     ┆ 5   ┆ 2024-02-05 │
    #   # │ 3     ┆ 6   ┆ 2024-03-06 │
    #   # └───────┴─────┴────────────┘
    #
    # @example We can also use `pl.date` for filtering:
    #   df = Polars::DataFrame.new(
    #     {
    #       "start" => [Date.new(2024, 1, 1), Date.new(2024, 1, 1), Date.new(2024, 1, 1)],
    #       "end" => [Date.new(2024, 5, 1), Date.new(2024, 7, 1), Date.new(2024, 9, 1)]
    #     }
    #   )
    #   df.filter(Polars.col("end") > Polars.date(2024, 6, 1))
    #   # =>
    #   # shape: (2, 2)
    #   # ┌────────────┬────────────┐
    #   # │ start      ┆ end        │
    #   # │ ---        ┆ ---        │
    #   # │ date       ┆ date       │
    #   # ╞════════════╪════════════╡
    #   # │ 2024-01-01 ┆ 2024-07-01 │
    #   # │ 2024-01-01 ┆ 2024-09-01 │
    #   # └────────────┴────────────┘
    def date(
      year,
      month,
      day
    )
      datetime(year, month, day).cast(Date).alias("date")
    end

    # Create a Polars literal expression of type Time.
    #
    # @param hour [Object]
    #   column or literal, ranging from 0-23.
    # @param minute [Object]
    #   column or literal, ranging from 0-59.
    # @param second [Object]
    #   column or literal, ranging from 0-59.
    # @param microsecond [Object]
    #   column or literal, ranging from 0-999999.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "hour" => [12, 13, 14],
    #       "minute" => [15, 30, 45]
    #     }
    #   )
    #   df.with_columns(Polars.time(Polars.col("hour"), Polars.col("minute")))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌──────┬────────┬──────────┐
    #   # │ hour ┆ minute ┆ time     │
    #   # │ ---  ┆ ---    ┆ ---      │
    #   # │ i64  ┆ i64    ┆ time     │
    #   # ╞══════╪════════╪══════════╡
    #   # │ 12   ┆ 15     ┆ 12:15:00 │
    #   # │ 13   ┆ 30     ┆ 13:30:00 │
    #   # │ 14   ┆ 45     ┆ 14:45:00 │
    #   # └──────┴────────┴──────────┘
    def time(
      hour = nil,
      minute = nil,
      second = nil,
      microsecond = nil
    )
      epoch_start = [1970, 1, 1]
      datetime(*epoch_start, hour, minute, second, microsecond)
        .cast(Time)
        .alias("time")
    end

    # Create polars `Duration` from distinct time components.
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "datetime" => [DateTime.new(2022, 1, 1), DateTime.new(2022, 1, 2)],
    #       "add" => [1, 2]
    #     }
    #   )
    #   df.select(
    #     [
    #       (Polars.col("datetime") + Polars.duration(weeks: "add")).alias("add_weeks"),
    #       (Polars.col("datetime") + Polars.duration(days: "add")).alias("add_days"),
    #       (Polars.col("datetime") + Polars.duration(seconds: "add")).alias("add_seconds"),
    #       (Polars.col("datetime") + Polars.duration(milliseconds: "add")).alias(
    #         "add_milliseconds"
    #       ),
    #       (Polars.col("datetime") + Polars.duration(hours: "add")).alias("add_hours")
    #     ]
    #   )
    #   # =>
    #   # shape: (2, 5)
    #   # ┌─────────────────────┬─────────────────────┬─────────────────────┬─────────────────────────┬─────────────────────┐
    #   # │ add_weeks           ┆ add_days            ┆ add_seconds         ┆ add_milliseconds        ┆ add_hours           │
    #   # │ ---                 ┆ ---                 ┆ ---                 ┆ ---                     ┆ ---                 │
    #   # │ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]        ┆ datetime[ns]            ┆ datetime[ns]        │
    #   # ╞═════════════════════╪═════════════════════╪═════════════════════╪═════════════════════════╪═════════════════════╡
    #   # │ 2022-01-08 00:00:00 ┆ 2022-01-02 00:00:00 ┆ 2022-01-01 00:00:01 ┆ 2022-01-01 00:00:00.001 ┆ 2022-01-01 01:00:00 │
    #   # │ 2022-01-16 00:00:00 ┆ 2022-01-04 00:00:00 ┆ 2022-01-02 00:00:02 ┆ 2022-01-02 00:00:00.002 ┆ 2022-01-02 02:00:00 │
    #   # └─────────────────────┴─────────────────────┴─────────────────────┴─────────────────────────┴─────────────────────┘
    def duration(
      weeks: nil,
      days: nil,
      hours: nil,
      minutes: nil,
      seconds: nil,
      milliseconds: nil,
      microseconds: nil,
      nanoseconds: nil,
      time_unit: nil
    )
      if !nanoseconds.nil? && time_unit.nil?
        time_unit = "ns"
      end

      if !weeks.nil?
        weeks = Utils.parse_into_expression(weeks, str_as_lit: false)
      end
      if !days.nil?
        days = Utils.parse_into_expression(days, str_as_lit: false)
      end
      if !hours.nil?
        hours = Utils.parse_into_expression(hours, str_as_lit: false)
      end
      if !minutes.nil?
        minutes = Utils.parse_into_expression(minutes, str_as_lit: false)
      end
      if !seconds.nil?
        seconds = Utils.parse_into_expression(seconds, str_as_lit: false)
      end
      if !milliseconds.nil?
        milliseconds = Utils.parse_into_expression(milliseconds, str_as_lit: false)
      end
      if !microseconds.nil?
        microseconds = Utils.parse_into_expression(microseconds, str_as_lit: false)
      end
      if !nanoseconds.nil?
        nanoseconds = Utils.parse_into_expression(nanoseconds, str_as_lit: false)
      end

      if time_unit.nil?
        time_unit = "us"
      end

      Utils.wrap_expr(
        Plr.duration(
          weeks,
          days,
          hours,
          minutes,
          seconds,
          milliseconds,
          microseconds,
          nanoseconds,
          time_unit
        )
      )
    end

    # Concat the arrays in a Series dtype List in linear time.
    #
    # @return [Expr]
    #
    # @example Concatenate two existing list columns. Null values are propagated.
    #   df = Polars::DataFrame.new({"a" => [[1, 2], [3], [4, 5]], "b" => [[4], [], nil]})
    #   df.with_columns(concat_list: Polars.concat_list("a", "b"))
    #   # =>
    #   # shape: (3, 3)
    #   # ┌───────────┬───────────┬─────────────┐
    #   # │ a         ┆ b         ┆ concat_list │
    #   # │ ---       ┆ ---       ┆ ---         │
    #   # │ list[i64] ┆ list[i64] ┆ list[i64]   │
    #   # ╞═══════════╪═══════════╪═════════════╡
    #   # │ [1, 2]    ┆ [4]       ┆ [1, 2, 4]   │
    #   # │ [3]       ┆ []        ┆ [3]         │
    #   # │ [4, 5]    ┆ null      ┆ null        │
    #   # └───────────┴───────────┴─────────────┘
    #
    # @example Non-list columns are cast to a list before concatenation. The output data type is the supertype of the concatenated columns.
    #   df.select("a", concat_list: Polars.concat_list("a", Polars.lit("x")))
    #   # =>
    #   # shape: (3, 2)
    #   # ┌───────────┬─────────────────┐
    #   # │ a         ┆ concat_list     │
    #   # │ ---       ┆ ---             │
    #   # │ list[i64] ┆ list[str]       │
    #   # ╞═══════════╪═════════════════╡
    #   # │ [1, 2]    ┆ ["1", "2", "x"] │
    #   # │ [3]       ┆ ["3", "x"]      │
    #   # │ [4, 5]    ┆ ["4", "5", "x"] │
    #   # └───────────┴─────────────────┘
    #
    # @example Create lagged columns and collect them into a list. This mimics a rolling window.
    #   df = Polars::DataFrame.new({"A" => [1.0, 2.0, 9.0, 2.0, 13.0]})
    #   df = df.select(3.times.map { |i| Polars.col("A").shift(i).alias("A_lag_#{i}") })
    #   df.select(
    #     Polars.concat_list(3.times.map { |i| "A_lag_#{i}" }.reverse).alias("A_rolling")
    #   )
    #   # =>
    #   # shape: (5, 1)
    #   # ┌───────────────────┐
    #   # │ A_rolling         │
    #   # │ ---               │
    #   # │ list[f64]         │
    #   # ╞═══════════════════╡
    #   # │ [null, null, 1.0] │
    #   # │ [null, 1.0, 2.0]  │
    #   # │ [1.0, 2.0, 9.0]   │
    #   # │ [2.0, 9.0, 2.0]   │
    #   # │ [9.0, 2.0, 13.0]  │
    #   # └───────────────────┘
    def concat_list(exprs, *more_exprs)
      exprs = Utils.parse_into_list_of_expressions(exprs, *more_exprs)
      Utils.wrap_expr(Plr.concat_list(exprs))
    end

    # Horizontally concatenate columns into a single array column.
    #
    # Non-array columns are reshaped to a unit-width array. All columns must have
    # a dtype of either `Polars::Array.new(<DataType>, width)` or `Polars::<DataType>`.
    #
    # @note
    #   This functionality is considered **unstable**. It may be changed
    #   at any point without it being considered a breaking change.
    #
    # @param exprs [Object]
    #   Columns to concatenate into a single array column. Accepts expression input.
    #   Strings are parsed as column names, other non-expression inputs are parsed as
    #   literals.
    # @param more_exprs [Array]
    #   Additional columns to concatenate into a single array column, specified as
    #   positional arguments.
    #
    # @return [Expr]
    def concat_arr(exprs, *more_exprs)
      exprs = Utils.parse_into_list_of_expressions(exprs, *more_exprs)
      Utils.wrap_expr(Plr.concat_arr(exprs))
    end

    # Collect several columns into a Series of dtype Struct.
    #
    # @param exprs [Array]
    #   Column(s) to collect into a struct column, specified as positional arguments.
    #   Accepts expression input. Strings are parsed as column names,
    #   other non-expression inputs are parsed as literals.
    # @param schema [Hash]
    #   Optional schema that explicitly defines the struct field dtypes. If no columns
    #   or expressions are provided, schema keys are used to define columns.
    # @param eager [Boolean]
    #   Evaluate immediately and return a `Series`. If set to `false` (default),
    #   return an expression instead.
    # @param named_exprs [Hash]
    #   Additional columns to collect into the struct column, specified as keyword
    #   arguments. The columns will be renamed to the keyword used.
    #
    # @return [Object]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "int" => [1, 2],
    #       "str" => ["a", "b"],
    #       "bool" => [true, nil],
    #       "list" => [[1, 2], [3]],
    #     }
    #   )
    #   df.select([Polars.struct(Polars.all).alias("my_struct")])
    #   # =>
    #   # shape: (2, 1)
    #   # ┌─────────────────────┐
    #   # │ my_struct           │
    #   # │ ---                 │
    #   # │ struct[4]           │
    #   # ╞═════════════════════╡
    #   # │ {1,"a",true,[1, 2]} │
    #   # │ {2,"b",null,[3]}    │
    #   # └─────────────────────┘
    #
    # @example Collect selected columns into a struct by either passing a list of columns, or by specifying each column as a positional argument.
    #   df.select(Polars.struct("int", false).alias("my_struct"))
    #   # =>
    #   # shape: (2, 1)
    #   # ┌───────────┐
    #   # │ my_struct │
    #   # │ ---       │
    #   # │ struct[2] │
    #   # ╞═══════════╡
    #   # │ {1,false} │
    #   # │ {2,false} │
    #   # └───────────┘
    #
    # @example Use keyword arguments to easily name each struct field.
    #   df.select(Polars.struct(p: "int", q: "bool").alias("my_struct")).schema
    #   # => Polars::Schema({"my_struct"=>Polars::Struct({"p"=>Polars::Int64, "q"=>Polars::Boolean})})
    def struct(*exprs, schema: nil, eager: false, **named_exprs)
      rbexprs = Utils.parse_into_list_of_expressions(*exprs, **named_exprs)
      expr = Utils.wrap_expr(Plr.as_struct(rbexprs))

      if !schema.nil? && !schema.empty?
        if !exprs.any?
          # no columns or expressions provided; create one from schema keys
          expr =
            Utils.wrap_expr(
              Plr.as_struct(Utils.parse_into_list_of_expressions(schema.keys))
            )
          expr = expr.cast(Struct.new(schema), strict: false)
        end
      end

      if eager
        Polars.select(expr).to_series
      else
        expr
      end
    end

    # Horizontally concat Utf8 Series in linear time. Non-Utf8 columns are cast to Utf8.
    #
    # @param exprs [Object]
    #   Columns to concat into a Utf8 Series.
    # @param more_exprs [Array]
    #   Additional columns to concatenate into a single string column, specified as
    #   positional arguments.
    # @param separator [String]
    #   String value that will be used to separate the values.
    # @param ignore_nulls [Boolean]
    #   Ignore null values (default).
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a" => [1, 2, 3],
    #       "b" => ["dogs", "cats", nil],
    #       "c" => ["play", "swim", "walk"]
    #     }
    #   )
    #   df.with_columns(
    #     [
    #       Polars.concat_str(
    #         [
    #           Polars.col("a") * 2,
    #           Polars.col("b"),
    #           Polars.col("c")
    #         ],
    #         separator: " "
    #       ).alias("full_sentence")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 4)
    #   # ┌─────┬──────┬──────┬───────────────┐
    #   # │ a   ┆ b    ┆ c    ┆ full_sentence │
    #   # │ --- ┆ ---  ┆ ---  ┆ ---           │
    #   # │ i64 ┆ str  ┆ str  ┆ str           │
    #   # ╞═════╪══════╪══════╪═══════════════╡
    #   # │ 1   ┆ dogs ┆ play ┆ 2 dogs play   │
    #   # │ 2   ┆ cats ┆ swim ┆ 4 cats swim   │
    #   # │ 3   ┆ null ┆ walk ┆ null          │
    #   # └─────┴──────┴──────┴───────────────┘
    def concat_str(exprs, *more_exprs, separator: "", ignore_nulls: false)
      exprs = Utils.parse_into_list_of_expressions(exprs, *more_exprs)
      Utils.wrap_expr(Plr.concat_str(exprs, separator, ignore_nulls))
    end

    # Format expressions as a string.
    #
    # @param f_string [String]
    #   A string that with placeholders.
    #   For example: "hello_{}" or "{}_world
    # @param args [Object]
    #   Expression(s) that fill the placeholders
    #
    # @return [Expr]
    #
    # @example
    #   df = Polars::DataFrame.new(
    #     {
    #       "a": ["a", "b", "c"],
    #       "b": [1, 2, 3]
    #     }
    #   )
    #   df.select(
    #     [
    #       Polars.format("foo_{}_bar_{}", Polars.col("a"), "b").alias("fmt")
    #     ]
    #   )
    #   # =>
    #   # shape: (3, 1)
    #   # ┌─────────────┐
    #   # │ fmt         │
    #   # │ ---         │
    #   # │ str         │
    #   # ╞═════════════╡
    #   # │ foo_a_bar_1 │
    #   # │ foo_b_bar_2 │
    #   # │ foo_c_bar_3 │
    #   # └─────────────┘
    def format(f_string, *args)
      if f_string.scan("{}").length != args.length
        raise ArgumentError, "number of placeholders should equal the number of arguments"
      end

      exprs = []

      arguments = args.each
      f_string.split(/(\{\})/).each do |s|
        if s == "{}"
          e = Utils.wrap_expr(Utils.parse_into_expression(arguments.next))
          exprs << e
        elsif s.length > 0
          exprs << lit(s)
        end
      end

      concat_str(exprs, separator: "")
    end
  end
end

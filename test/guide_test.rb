require_relative "test_helper"

class GuideTest < Minitest::Test
  # https://github.com/pola-rs/polars
  def test_readme
    df = Polars::DataFrame.new({
      "A" => [1, 2, 3, 4, 5],
      "fruits" => ["banana", "banana", "apple", "apple", "banana"],
      "B" => [5, 4, 3, 2, 1],
      "cars" => ["beetle", "audi", "beetle", "beetle", "beetle"]
    })

    output df.sort("fruits").select([
      "fruits",
      "cars",
      Polars.lit("fruits").alias("literal_string_fruits"),
      Polars.col("B").filter(Polars.col("cars") == "beetle").sum,
      Polars.col("A").filter(Polars.col("B") > 2).sum.over("cars").alias("sum_A_by_cars"),
      Polars.col("A").sum.over("fruits").alias("sum_A_by_fruits"),
      Polars.col("A").reverse.over("fruits").alias("rev_A_by_fruits"),
      Polars.col("A").sort_by("B").over("fruits").alias("sort_A_by_B_by_fruits")
    ])
  end

  # https://pola-rs.github.io/polars-book/user-guide/quickstart/intro.html
  def test_quickstart
    Polars.read_csv("test/support/iris.csv")
      .filter(Polars.col("sepal_length") > 5)
      .group_by("species")
      .agg(Polars.all.sum)

    Polars.read_csv("test/support/iris.csv")
      .lazy
      .filter(Polars.col("sepal_length") > 5)
      .group_by("species")
      .agg(Polars.all.sum)
      .collect
  end

  # https://pola-rs.github.io/polars-book/user-guide/dsl/expressions.html
  def test_expressions
    Polars.col("foo").sort.head(2)

    df = Polars::DataFrame.new({
      "nrs" => [1, 2, 3, nil, 5],
      "names" => ["foo", "ham", "spam", "egg", nil],
      "random" => 5.times.map { rand },
      "groups" => ["A", "A", "B", "C", "B"]
    })
    output df

    output df.select([
      Polars.col("names").n_unique.alias("unique_names_1"),
      Polars.col("names").unique.count.alias("unique_names_2")
    ])

    output df.select([
      Polars.sum("random").alias("sum"),
      Polars.min("random").alias("min"),
      Polars.max("random").alias("max"),
      Polars.col("random").max.alias("other_max"),
      Polars.std("random").alias("std dev"),
      Polars.var("random").alias("variance")
    ])

    output df.select([
      Polars.col("names").filter(Polars.col("names").str.contains("am$")).count
    ])

    output df.select([
      Polars.when(Polars.col("random") > 0.5).then(0).otherwise(Polars.col("random")) * Polars.sum("nrs")
    ])

    output df.select([
      Polars.col("*"),
      Polars.col("random").sum.over("groups").alias("sum[random]/groups"),
      Polars.col("random").implode.over("names").alias("random/name")
    ])
  end

  # https://pola-rs.github.io/polars-book/user-guide/dsl/contexts.html
  def test_contexts
    df = Polars::DataFrame.new({
      "nrs" => [1, 2, 3, nil, 5],
      "names" => ["foo", "ham", "spam", "egg", nil],
      "random" => 5.times.map { rand },
      "groups" => ["A", "A", "B", "C", "B"]
    })

    output df.select([
      Polars.sum("nrs"),
      Polars.col("names").sort,
      Polars.col("names").first.alias("first name"),
      (Polars.mean("nrs") * 10).alias("10xnrs")
    ])

    output df.with_columns([
      Polars.sum("nrs").alias("nrs_sum"),
      Polars.col("random").count.alias("count")
    ])

    output df.group_by("groups").agg([
      Polars.sum("nrs"),
      Polars.col("random").count.alias("count"),
      Polars.col("random").filter(Polars.col("names").is_not_null).sum.suffix("_sum"),
      Polars.col("names").reverse.alias(("reversed names"))
    ])
  end

  # https://pola-rs.github.io/polars-book/user-guide/dsl/list_context.html
  def test_list_context
    grades = Polars::DataFrame.new({
      "student" => ["bas", "laura", "tim", "jenny"],
      "arithmetic" => [10, 5, 6, 8],
      "biology" => [4, 6, 2, 7],
      "geography" => [8, 4, 9, 7]
    })
    output grades

    output grades.select([Polars.concat_list(Polars.all.exclude("student")).alias("all_grades")])

    rank_pct = Polars.element.rank(descending: true) / Polars.col("").count
    output grades.with_columns(
        Polars.concat_list(Polars.all.exclude("student")).alias("all_grades")
      ).select([
        Polars.all.exclude("all_grades"),
        Polars.col("all_grades").list.eval(rank_pct).alias("grades_rank")
      ])
  end

  # https://pola-rs.github.io/polars-book/user-guide/notebooks/introduction_polars-py.html
  def test_examples
    df = Polars::DataFrame.new({
      "A" => [1, 2, 3, 4, 5],
      "fruits" => ["banana", "banana", "apple", "apple", "banana"],
      "B" => [5, 4, 3, 2, 1],
      "cars" => ["beetle", "audi", "beetle", "beetle", "beetle"],
      "optional" => [28, 300, nil, 2, -30]
    })
    output df

    output df.select([
      Polars.col("A"),
      "B",
      Polars.lit("B"),
      Polars.col("fruits")
    ])

    output df.select([
      Polars.col("^A|B$").sum
    ])

    output df.select([
      Polars.col(["A", "B"]).sum
    ])

    output df.select([
      Polars.all,
      Polars.all.reverse.suffix("_reverse")
    ])

    output df.select([
      Polars.all
    ])

    predicate = Polars.col("fruits").str.contains("^b.*")
    output df.select([predicate])

    output df.filter(predicate)

    output df.select([
      Polars.col("A").filter(Polars.col("fruits").str.contains("^b.*")).sum,
      (Polars.col("B").filter(Polars.col("cars").str.contains("^b.*")).sum * Polars.col("B").sum).alias("some_compute()")
    ])

    some_var = 1
    output df.select([
      ((Polars.col("A") / 124.0 * Polars.col("B")) / Polars.sum("B") * some_var).alias("computed")
    ])

    output df.select([
      "fruits",
      "B",
      Polars.when(Polars.col("fruits") == "banana").then(Polars.col("B")).otherwise(-1).alias("b")
    ])

    # output df.select([
    #   "A",
    #   "B",
    #   Polars.fold(0, ->(a, b) { a + b }, [Polars.col("A"), "B", Polars.col("B")**2, Polars.col("A") / 2.0]).alias("fold")
    # ])

    # output df.select([
    #   Polars.arange(0, df.height).alias("idx"),
    #   "A",
    #   Polars.col("A").shift.alias("A_shifted"),
    #   Polars.concat_str(Polars.all, "-").alias("str_concat_1"),
    #   Polars.fold(Polars.col("A"), ->(a, b) { a + "-" + b }, Polars.all.exclude("A")).alias("str_concat_2")
    # ])

    output df.sort("cars").group_by("fruits")
      .agg([
        Polars.col("B").sum.alias("B_sum"),
        Polars.sum("B").alias("B_sum2"),
        Polars.first("fruits").alias("fruits_first"),
        Polars.count("A").alias("count"),
        Polars.col("cars").reverse
      ])

    output df.sort("cars").group_by("fruits")
      .agg([
        Polars.col("B").sum.alias("B_sum"),
        Polars.sum("B").alias("B_sum2"),
        Polars.first("fruits").alias("fruits_first"),
        Polars.count("A").alias("count"),
        Polars.col("cars").reverse
      ]).explode("cars")

    output df.group_by("fruits")
      .agg([
        Polars.col("B").sum.alias("B_sum"),
        Polars.sum("B").alias("B_sum2"),
        Polars.first("fruits").alias("fruits_first"),
        Polars.len,
        Polars.col("B").shift.alias("B_shifted")
      ]).explode("B_shifted")

    output df.sort("cars").group_by("fruits")
      .agg([
        Polars.col("B").sum,
        Polars.sum("B").alias("B_sum2"),
        Polars.first("fruits").alias("fruits_first"),
        Polars.count("A").alias("count"),
        Polars.col("cars").reverse
      ]).explode("cars")

    output df.group_by("fruits")
      .agg([
        Polars.col("B").shift.alias("shift_B"),
        Polars.col("B").reverse.alias("rev_B")
      ])

    output df.group_by("fruits")
      .agg([
        Polars.col("B").filter(Polars.col("B") > 1).implode.keep_name
      ])

    output df.group_by("fruits")
      .agg([
        Polars.col("B").filter(Polars.col("B") > 1).mean
      ])

    output df.group_by("fruits")
      .agg([
        Polars.col("B").shift(1, fill_value: 0).alias("shifted"),
        Polars.col("B").shift(1, fill_value: 0).sum.alias("shifted_sum")
      ])

    output df.select([
      "fruits",
      "cars",
      "B",
      Polars.col("B").sum.over("fruits").alias("B_sum_by_fruits"),
      Polars.col("B").sum.over("cars").alias("B_sum_by_cars")
    ])

    output df.select([
      "fruits",
      "B",
      Polars.col("B").reverse.over("fruits").alias("B_reversed_by_fruits")
    ])

    output df.select([
      "fruits",
      "B",
      Polars.col("B").shift.over("fruits").alias("lag_B_by_fruits")
    ])
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/selecting_data/selecting_data_expressions.html
  def test_selecting_data_expressions
    df = Polars::DataFrame.new({
      "id" => [1, 2, 3],
      "color" => ["blue", "red", "green"],
      "size" => ["small", "medium", "large"]
    })
    output df

    output df.filter(Polars.col("id") <= 2)

    output df.filter((Polars.col("id") <= 2) & (Polars.col("size") == "small"))

    output df.select("id")

    output df.select(["id", "color"])

    output df.select(Polars.col("^col.*$"))

    output df.select(Polars.col(Polars::Int64))

    output df.filter(Polars.col("id") <= 2).select(["id", "color"])
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/data/strings.html
  def test_data_strings
    df = Polars::DataFrame.new({"shakespeare" => "All that glitters is not gold".split(" ")})
    output df.with_columns(Polars.col("shakespeare").str.lengths.alias("letter_count"))

    df = Polars::DataFrame.new({"a" => "The man that ate a whole cake".split(" ")})
    output df.filter(Polars.col("a").str.contains("(?i)^the$|^a$").is_not)
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/missing_data.html
  def test_missing_data
    df = Polars::DataFrame.new({"value" => [1, nil]})
    output df

    output df.null_count

    output df.select(
      Polars.col("value").is_null
    )

    df = Polars::DataFrame.new({
      "col1" => [1, 2, 3],
      "col2" => [1, nil, 3]
    })
    output df

    output df.with_columns(
      Polars.col("col2").fill_null(Polars.lit(2))
    )

    output df.with_columns(
      Polars.col("col2").fill_null(strategy: "forward")
    )

    output df.with_columns(
      Polars.col("col2").fill_null(Polars.median("col2"))
    )

    output df.with_columns(
      Polars.col("col2").interpolate
    )

    nan_df = Polars::DataFrame.new({"value" => [1.0, Float::NAN, Float::NAN, 3.0]})
    output nan_df

    output nan_df.with_columns(
      Polars.col("value").fill_nan(nil).alias("value")
    ).mean
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/combining_data/concatenating.html
  def test_concatenation
    df_v1 = Polars::DataFrame.new({a: [1], b: [3]})
    df_v2 = Polars::DataFrame.new({a: [2], b: [4]})
    output Polars.concat([df_v1, df_v2], how: "vertical")

    df_h1 = Polars::DataFrame.new({l1: [1, 2], l2: [3, 4]})
    df_h2 = Polars::DataFrame.new({r1: [5, 6], r2: [7, 8], r3: [9, 10]})
    output Polars.concat([df_h1, df_h2], how: "horizontal")

    df_d1 = Polars::DataFrame.new({a: [1], b: [3]})
    df_d2 = Polars::DataFrame.new({a: [2], d: [4]})
    output Polars.concat([df_d1, df_d2], how: "diagonal")
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/combining_data/joining.html
  def test_joining
    df_cars = Polars::DataFrame.new({id: ["a", "b", "c"], make: ["ford", "toyota", "bmw"]})
    output df_cars

    df_repairs = Polars::DataFrame.new({id: ["c", "c"], cost: [100, 200]})
    output df_repairs

    output df_cars.join(df_repairs, on: "id", how: "inner")

    output df_cars.join(df_repairs, on: "id", how: "semi")

    output df_cars.join(df_repairs, on: "id", how: "anti")
  end

  def output(value)
    p value if ENV["VERBOSE"]
  end
end

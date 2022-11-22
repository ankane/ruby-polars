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

    df.sort("fruits").select([
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
      .groupby("species")
      .agg(Polars.all.sum)

    Polars.read_csv("test/support/iris.csv")
      .lazy
      .filter(Polars.col("sepal_length") > 5)
      .groupby("species")
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

    df.select([
      Polars.col("names").n_unique.alias("unique_names_1"),
      Polars.col("names").unique.count.alias("unique_names_2")
    ])

    df.select([
      Polars.sum("random").alias("sum"),
      Polars.min("random").alias("min"),
      Polars.max("random").alias("max"),
      Polars.col("random").max.alias("other_max"),
      Polars.std("random").alias("std dev"),
      Polars.var("random").alias("variance")
    ])

    df.select([
      Polars.col("names").filter(Polars.col("names").str.contains("am$")).count
    ])

    df.select([
      Polars.when(Polars.col("random") > 0.5).then(0).otherwise(Polars.col("random")) * Polars.sum("nrs")
    ])

    df.select([
      Polars.col("*"),
      Polars.col("random").sum.over("groups").alias("sum[random]/groups"),
      Polars.col("random").list.over("names").alias("random/name")
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

    df.groupby("foo").agg([Polars.col("bar").sum])

    df.select([
      Polars.sum("nrs"),
      Polars.col("names").sort,
      Polars.col("names").first.alias("first name"),
      (Polars.mean("nrs") * 10).alias("10xnrs")
    ])

    df.with_columns([
      Polars.sum("nrs").alias("nrs_sum"),
      Polars.col("random").count.alias("count")
    ])

    df.groupby("groups").agg([
      Polars.sum("nrs"),
      Polars.col("random").count.alias("count"),
      Polars.col("random").filter(Polars.col("names").is_not_null).sum.suffix("_sum"),
      Polars.col("names").reverse.alias(("reversed names"))
    ])
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/selecting_data/selecting_data_expressions.html
  def test_selecting_data_expressions
    df = Polars::DataFrame.new({
      "id" => [1, 2, 3],
      "color" => ["blue", "red", "green"],
      "size" => ["small", "medium", "large"],
    })

    df.filter(Polars.col("id") <= 2)

    df.filter((Polars.col("id") <= 2) & (Polars.col("size") == "small"))

    df.select("id")

    df.select(["id", "color"])

    df.select(Polars.col("^col.*$"))

    # df.select(Polars.col(Polars::Int64))

    df.filter(Polars.col("id") <= 2).select(["id", "color"])
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/data/strings.html
  def test_data_strings
    df = Polars::DataFrame.new({"shakespeare" => "All that glitters is not gold".split(" ")})
    df.with_column(Polars.col("shakespeare").str.lengths.alias("letter_count"))

    df = Polars::DataFrame.new({"a" => "The man that ate a whole cake".split(" ")})
    df.filter(Polars.col("a").str.contains("(?i)^the$|^a$").is_not)
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/missing_data.html
  def test_missing_data
    df = Polars::DataFrame.new({"value" => [1, nil]})

    df.null_count

    df.select(
      Polars.col("value").is_null
    )

    df = Polars::DataFrame.new({
      "col1" => [1, 2, 3],
      "col2" => [1, nil, 3]
    })

    df.with_column(
      Polars.col("col2").fill_null(Polars.lit(2))
    )

    df.with_column(
      Polars.col("col2").fill_null(strategy: "forward")
    )

    df.with_column(
      Polars.col("col2").fill_null(Polars.median("col2"))
    )

    df.with_column(
      Polars.col("col2").interpolate
    )

    nan_df = Polars::DataFrame.new({"value" => [1.0, Float::NAN, Float::NAN, 3.0]})

    nan_df.with_column(
      Polars.col("value").fill_nan(nil).alias("value")
    ).mean
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/combining_data/concatenating.html
  def test_concatenation
    df_v1 = Polars::DataFrame.new({a: [1], b: [3]})
    df_v2 = Polars::DataFrame.new({a: [2], b: [4]})
    Polars.concat([df_v1, df_v2], how: "vertical")

    df_h1 = Polars::DataFrame.new({l1: [1, 2], l2: [3, 4]})
    df_h2 = Polars::DataFrame.new({r1: [5, 6], r2: [7, 8], r3: [9, 10]})
    Polars.concat([df_h1, df_h2], how: "horizontal")

    df_d1 = Polars::DataFrame.new({a: [1], b: [3]})
    df_d2 = Polars::DataFrame.new({a: [2], d: [4]})
    Polars.concat([df_d1, df_d2], how: "diagonal")
  end

  # https://pola-rs.github.io/polars-book/user-guide/howcani/combining_data/joining.html
  def test_joining
    df_cars = Polars::DataFrame.new({id: ["a", "b", "c"], make: ["ford", "toyota", "bmw"]})
    df_repairs = Polars::DataFrame.new({id: ["c", "c"], cost: [100, 200]})

    df_cars.join(df_repairs, on: "id", how: "inner")

    df_cars.join(df_repairs, on: "id", how: "semi")

    df_cars.join(df_repairs, on: "id", how: "anti")
  end
end

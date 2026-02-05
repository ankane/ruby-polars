# Ruby Polars

🔥 Blazingly fast DataFrames for Ruby, powered by [Polars](https://github.com/pola-rs/polars)

[![Build Status](https://github.com/ankane/ruby-polars/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/ruby-polars/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "polars-df"
```

## Getting Started

This library follows the [Polars Python API](https://docs.pola.rs/api/python/stable/reference/index.html).

```ruby
Polars.scan_csv("iris.csv")
  .filter(Polars.col("sepal_length") > 5)
  .group_by("species")
  .agg(Polars.all.sum)
  .collect
```

You can follow [Polars tutorials](https://docs.pola.rs/user-guide/getting-started/) and convert the code to Ruby in many cases. Feel free to open an issue if you run into problems.

## Reference

- [Series](https://www.rubydoc.info/gems/polars-df/Polars/Series)
- [DataFrame](https://www.rubydoc.info/gems/polars-df/Polars/DataFrame)
- [LazyFrame](https://www.rubydoc.info/gems/polars-df/Polars/LazyFrame)

## Examples

### Creating DataFrames

From a CSV

```ruby
Polars.read_csv("file.csv")

# or lazily with
Polars.scan_csv("file.csv")
```

From Parquet

```ruby
Polars.read_parquet("file.parquet")

# or lazily with
Polars.scan_parquet("file.parquet")
```

From Active Record

```ruby
Polars.read_database(User.all)
# or
Polars.read_database("SELECT * FROM users")
```

From JSON

```ruby
Polars.read_json("file.json")
# or
Polars.read_ndjson("file.ndjson")

# or lazily with
Polars.scan_ndjson("file.ndjson")
```

From Feather / Arrow IPC

```ruby
Polars.read_ipc("file.arrow")

# or lazily with
Polars.scan_ipc("file.arrow")
```

From Avro

```ruby
Polars.read_avro("file.avro")
```

From Iceberg (experimental, requires [iceberg](https://github.com/ankane/iceberg-ruby))

```ruby
Polars.scan_iceberg(table)
```

From Delta Lake (experimental, requires [deltalake-rb](https://github.com/ankane/delta-ruby))

```ruby
Polars.read_delta("./table")

# or lazily with
Polars.scan_delta("./table")
```

From a hash

```ruby
Polars::DataFrame.new({
  a: [1, 2, 3],
  b: ["one", "two", "three"]
})
```

From an array of hashes

```ruby
Polars::DataFrame.new([
  {a: 1, b: "one"},
  {a: 2, b: "two"},
  {a: 3, b: "three"}
])
```

From an array of series

```ruby
Polars::DataFrame.new([
  Polars::Series.new("a", [1, 2, 3]),
  Polars::Series.new("b", ["one", "two", "three"])
])
```

## Attributes

Get number of rows

```ruby
df.height
```

Get column names

```ruby
df.columns
```

Check if a column exists

```ruby
df.include?(name)
```

## Selecting Data

Select a column

```ruby
df["a"]
```

Select multiple columns

```ruby
df[["a", "b"]]
```

Select first rows

```ruby
df.head
```

Select last rows

```ruby
df.tail
```

## Filtering

Filter on a condition

```ruby
df.filter(Polars.col("a") == 2)
df.filter(Polars.col("a") != 2)
df.filter(Polars.col("a") > 2)
df.filter(Polars.col("a") >= 2)
df.filter(Polars.col("a") < 2)
df.filter(Polars.col("a") <= 2)
```

And, or, and exclusive or

```ruby
df.filter((Polars.col("a") > 1) & (Polars.col("b") == "two")) # and
df.filter((Polars.col("a") > 1) | (Polars.col("b") == "two")) # or
df.filter((Polars.col("a") > 1) ^ (Polars.col("b") == "two")) # xor
```

## Operations

Basic operations

```ruby
df["a"] + 5
df["a"] - 5
df["a"] * 5
df["a"] / 5
df["a"] % 5
df["a"] ** 2
df["a"].sqrt
df["a"].abs
```

Rounding

```ruby
df["a"].round(2)
df["a"].ceil
df["a"].floor
```

Logarithm

```ruby
df["a"].log # natural log
df["a"].log(10)
```

Exponentiation

```ruby
df["a"].exp
```

Trigonometric functions

```ruby
df["a"].sin
df["a"].cos
df["a"].tan
df["a"].arcsin
df["a"].arccos
df["a"].arctan
```

Hyperbolic functions

```ruby
df["a"].sinh
df["a"].cosh
df["a"].tanh
df["a"].arcsinh
df["a"].arccosh
df["a"].arctanh
```

Summary statistics

```ruby
df["a"].sum
df["a"].mean
df["a"].median
df["a"].quantile(0.90)
df["a"].min
df["a"].max
df["a"].std
df["a"].var
```

## Grouping

Group

```ruby
df.group_by("a").count
```

Works with all summary statistics

```ruby
df.group_by("a").max
```

Multiple groups

```ruby
df.group_by(["a", "b"]).count
```

## Combining Data Frames

Add rows

```ruby
df.vstack(other_df)
```

Add columns

```ruby
df.hstack(other_df)
```

Inner join

```ruby
df.join(other_df, on: "a")
```

Left join

```ruby
df.join(other_df, on: "a", how: "left")
```

## Encoding

One-hot encoding

```ruby
df.to_dummies
```

## Conversion

Array of hashes

```ruby
df.to_a
```

Hash of series

```ruby
df.to_h
```

CSV

```ruby
df.to_csv
# or
df.write_csv("file.csv")
```

Parquet

```ruby
df.write_parquet("file.parquet")
```

JSON

```ruby
df.write_json("file.json")
# or
df.write_ndjson("file.ndjson")
```

Feather / Arrow IPC

```ruby
df.write_ipc("file.arrow")
```

Avro

```ruby
df.write_avro("file.avro")
```

Iceberg (experimental)

```ruby
df.write_iceberg(table, mode: "append")
```

Delta Lake (experimental)

```ruby
df.write_delta("./table")
```

Numo array

```ruby
df.to_numo
```

## Types

You can specify column types when creating a data frame

```ruby
Polars::DataFrame.new(data, schema: {"a" => Polars::Int32, "b" => Polars::Float32})
```

Supported types are:

- boolean - `Boolean`
- decimal - `Decimal`
- float - `Float32`, `Float64`
- integer - `Int8`, `Int16`, `Int32`, `Int64`, `Int128`
- unsigned integer - `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`
- string - `String`, `Categorical`, `Enum`
- temporal - `Date`, `Datetime`, `Duration`, `Time`
- nested - `Array`, `List`, `Struct`
- other - `Binary`, `Object`, `Null`, `Unknown`

Get column types

```ruby
df.schema
```

For a specific column

```ruby
df["a"].dtype
```

Cast a column

```ruby
df["a"].cast(Polars::Int32)
```

## Visualization

Add [Vega](https://github.com/ankane/vega-ruby) to your application’s Gemfile:

```ruby
gem "vega"
```

And use:

```ruby
df.plot.line("a", "b")
```

Supports `line`, `pie`, `column`, `bar`, `area`, and `scatter` plots

Group data

```ruby
df.plot.line("a", "b", color: "c")
```

Stacked columns or bars

```ruby
df.plot.column("a", "b", color: "c", stacked: true)
```

Plot a series

```ruby
df["a"].plot.hist
```

Supports `hist`, `kde`, and `line` plots

## History

View the [changelog](https://github.com/ankane/ruby-polars/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/ruby-polars/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/ruby-polars/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/ruby-polars.git
cd ruby-polars
bundle install
bundle exec rake compile
bundle exec rake test
bundle exec rake test:docs
```

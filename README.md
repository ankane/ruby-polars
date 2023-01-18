# Polars Ruby

:fire: Blazingly fast DataFrames for Ruby, powered by [Polars](https://github.com/pola-rs/polars)

[![Build Status](https://github.com/ankane/polars-ruby/workflows/build/badge.svg?branch=master)](https://github.com/ankane/polars-ruby/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "polars-df"
```

## Getting Started

This library follows the [Polars Python API](https://pola-rs.github.io/polars/py-polars/html/reference/index.html).

```ruby
Polars.read_csv("iris.csv")
  .lazy
  .filter(Polars.col("sepal_length") > 5)
  .groupby("species")
  .agg(Polars.all.sum)
  .collect
```

You can follow [Polars tutorials](https://pola-rs.github.io/polars-book/user-guide/introduction.html) and convert the code to Ruby in many cases. Feel free to open an issue if you run into problems.

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
```

From Active Record

```ruby
Polars.read_sql(User.all)
```

From a hash

```ruby
Polars::DataFrame.new({
  a: [1, 2, 3],
  b: ["one", "two", "three"]
})
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
df[Polars.col("a") == 2]
df[Polars.col("a") != 2]
df[Polars.col("a") > 2]
df[Polars.col("a") >= 2]
df[Polars.col("a") < 2]
df[Polars.col("a") <= 2]
```

And, or, and exclusive or

```ruby
df[(Polars.col("a") > 1) & (Polars.col("b") == "two")] # and
df[(Polars.col("a") > 1) | (Polars.col("b") == "two")] # or
df[(Polars.col("a") > 1) ^ (Polars.col("b") == "two")] # xor
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
df["a"].asin
df["a"].acos
df["a"].atan
```

Hyperbolic functions

```ruby
df["a"].sinh
df["a"].cosh
df["a"].tanh
df["a"].asinh
df["a"].acosh
df["a"].atanh
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
df.groupby("a").count
```

Works with all summary statistics

```ruby
df.groupby("a").max
```

Multiple groups

```ruby
df.groupby(["a", "b"]).count
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

Array of rows

```ruby
df.rows
```

Hash of series

```ruby
df.to_h
```

CSV

```ruby
df.to_csv
# or
df.write_csv("data.csv")
```

Parquet

```ruby
df.write_parquet("data.parquet")
```

## Types

You can specify column types when creating a data frame

```ruby
Polars::DataFrame.new(data, columns: {"a" => Polars::Int32, "b" => Polars::Float32})
```

Supported types are:

- boolean - `Boolean`
- float - `Float64`, `Float32`
- integer - `Int64`, `Int32`, `Int16`, `Int8`
- unsigned integer - `UInt64`, `UInt32`, `UInt16`, `UInt8`
- string - `Utf8`, `Categorical`
- temporal - `Date`, `Datetime`, `Time`, `Duration`

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

## History

View the [changelog](CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/polars-ruby/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/polars-ruby/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/polars-ruby.git
cd polars-ruby
bundle install
bundle exec rake compile
bundle exec rake test
```

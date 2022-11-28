# Polars Ruby

:fire: Blazingly fast DataFrames for Ruby, powered by [Polars](https://github.com/pola-rs/polars)

[![Build Status](https://github.com/ankane/polars-ruby/workflows/build/badge.svg?branch=master)](https://github.com/ankane/polars-ruby/actions)

## Installation

Add this line to your application’s Gemfile:

```ruby
gem "polars-df"
```

Note: Rust is currently required for installation, and it can take 15-20 minutes to compile the extension.

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

You can follow [Polars tutorials](https://pola-rs.github.io/polars-book/user-guide/introduction.html) and convert the code to Ruby in many cases. Feel free to open an issue if you run into problems. Some methods are missing at the moment.

## Examples

### Creating DataFrames

From a CSV

```ruby
Polars.read_csv("file.csv")
```

From Parquet

```ruby
Polars.read_parquet("file.parquet")
```

From Active Record

```ruby
Polars::DataFrame.new(User.all)
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

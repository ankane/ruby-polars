## 0.8.1 (unreleased)

- Added `schema_overrides` option to `read_database` method
- Fixed error with `BigDecimal` objects

## 0.8.0 (2024-01-10)

- Updated Polars to 0.36.2
- Added support for Ruby 3.3
- Added warning to `count` method for `Series` and `Expr` about excluding null values in 0.9.0
- Added `cut` and `qcut` methods to `Series` and `Expr`
- Added `rle` and `rle_id` methods to `Series` and `Expr`
- Added `bottom_k` method to `Series`
- Aliased `Utf8` data type to `String`
- Fixed error with `top_k` method
- Dropped support for Ruby < 3.1

## 0.7.0 (2023-11-17)

- Updated Polars to 0.35.2
- Added support for SQL querying
- Added `!` for `Expr`
- Added `Config` module
- Added `none?` method to `Series`
- Aliased `groupby` to `group` for `DataFrame` and `LazyFrame`
- Changed series creation with all `nil` objects to `Null` type
- Removed `tz_localize` method from `DateTimeExpr`
- Removed support for passing Active Record objects to `DataFrame.new` (use `read_database` instead)

## 0.6.0 (2023-07-23)

- Updated Polars to 0.31.1
- Added `Array` type
- Added support for creating series with `Datetime` type
- Added support for `Null` to `to_a` method
- Improved support for `Decimal` and `Time` types
- Changed `arr` to `list` for `Series` and `Expr`
- Changed series creation with `BigDecimal` objects to `Decimal` type
- Changed series creation with `ActiveSupport::TimeWithZone` objects to `Datetime` type
- Changed equality for data types
- Fixed error with `groupby_dynamic` method
- Removed `agg_list` method from `GroupBy`

## 0.5.0 (2023-05-15)

- Updated Polars to 0.29.0
- Added support for `List` and `Struct` to `to_a` method
- Added support for creating series from Numo arrays
- Added column assignment to `DataFrame`
- Added `sort!` and `to_a` methods to `DataFrame`
- Added support for `Object` to `to_a` method
- Aliased `len` to `size` for `Series` and `DataFrame`
- Aliased `apply` to `map` and `unique` to `uniq` for `Series`
- Improved `any` and `all` for `Series`

## 0.4.0 (2023-04-01)

- Updated Polars to 0.28.0
- Added support for creating `Binary` series
- Added support for `Binary` to `to_a` method
- Added support for glob patterns to `read_parquet` method
- Added `sink_parquet` method to `LazyFrame`
- Added `BinaryExpr` and `BinaryNameSpace`
- Prefer `read_database` over `read_sql`

## 0.3.1 (2023-02-21)

- Added `plot` method to `DataFrame` and `GroupBy`
- Added `to_numo` method to `Series` and `DataFrame`
- Added support for `Datetime` to `to_a` method
- Fixed `is_datelike` method for `Datetime` and `Duration`

## 0.3.0 (2023-02-15)

- Updated Polars to 0.27.1
- Added `each` method to `Series`, `DataFrame`, and `GroupBy`
- Added `iter_rows` method to `DataFrame`
- Added `named` option to `row` and `rows` methods
- Replaced `include_bounds` option with `closed` for `is_between` method

## 0.2.5 (2023-02-01)

- Added support for glob patterns to `read_csv` method
- Added support for symbols to more methods

## 0.2.4 (2023-01-29)

- Added support for more types when creating a data frame from an array of hashes

## 0.2.3 (2023-01-22)

- Fixed error with precompiled gem on Mac ARM
- Fixed issue with structs

## 0.2.2 (2023-01-20)

- Added support for strings to `read_sql` method
- Improved indexing
- Fixed error with precompiled gem on Mac ARM

## 0.2.1 (2023-01-18)

- Added `read_sql` method
- Added `to_csv` method
- Added support for symbol keys

## 0.2.0 (2023-01-14)

- Updated Polars to 0.26.1
- Added precompiled gems for Linux and Mac
- Added data type classes
- Changed `dtype` and `schema` methods to return data type class instead of symbol
- Dropped support for Ruby < 3

## 0.1.5 (2022-12-22)

- Added `read_avro` and `write_avro` methods
- Added more methods

## 0.1.4 (2022-12-02)

- Added more methods
- Improved performance

## 0.1.3 (2022-11-27)

- Added more methods

## 0.1.2 (2022-11-25)

- Added more methods

## 0.1.1 (2022-11-23)

- Added more methods

## 0.1.0 (2022-11-21)

- First release

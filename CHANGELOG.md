## 0.24.0 (unreleased)

- Added `concat_arr` and `escape_regex` methods to `Polars`
- Added `rolling_rank` and `rolling_rank_by` methods to `Series` and `Expr`
- Added `days_in_month` method to `DateTimeExpr` and `DateTimeNameSpace`
- Added `agg` and `item` methods to `ListExpr` and `ListNameSpace`
- Added `agg` and `eval` methods to `ArrayExpr` and `ArrayNameSpace`
- Added `replace` method to `NameExpr`
- Added more options to `read_csv` and `scan_csv` methods
- Added more options to `over` method
- Changed `reverse` option to `descending`
- Changed `sep` option to `separator`
- Changed `row_count_name` option to `row_index_name`
- Changed `row_count_offset` option to `row_index_offset`
- Changed `min_periods` option to `min_samples`
- Changed `frac` option to `fraction` for `sample` methods
- Changed `drop_nulls` option to `ignore_nulls` for `all` and `any` methods
- Changed `join_nulls` option to `nulls_equal` for `join` method
- Changed `strict` option to `check_dtypes` for `equals` method
- Removed `warn_if_unsorted` option from `rolling_*_by` methods
- Removed `in_place` option from `rename` method
- Removed `append_chunks` option from `append` method
- Removed `utc` option from `strptime` method

## 0.23.0 (2025-11-04)

- Updated Polars to 0.52.0

## 0.22.0 (2025-09-17)

- Updated Polars to 0.51.0
- Added `serialize` and `deserialize` methods to `DataFrame`
- Added `storage_options` and `retries` options to `sink_ipc` method
- Added `business_day_count` method to `Polars`
- Added experimental support for Iceberg
- Added experimental `cast_options` option to `scan_parquet` method
- Changed `read_parquet_schema` method to return `Schema`
- Fixed `Object` type

## 0.21.1 (2025-08-18)

- Added `read_parquet_metadata` method to `Polars`
- Added more methods to `Series` and `Expr`
- Added more methods to `DataFrame` and `LazyFrame`
- Added more methods to `ArrayExpr` and `ArrayNameSpace`
- Added more methods to `BinaryExpr` and `BinaryNameSpace`
- Added more methods to `CatExpr` and `CatNameSpace`
- Added more methods to `DateTimeExpr` ane `DateTimeNameSpace`
- Added more methods to `ListExpr` and `ListNameSpace`
- Added more methods to `MetaExpr`
- Added more methods to `NameExpr`
- Added more methods to `StringExpr` and `StringNameSpace`
- Added more methods to `StructExpr` and `StructNameSpace`
- Fixed `subset` option for `drop_nulls` method

## 0.21.0 (2025-08-03)

- Updated Polars to 0.50.0
- Added `Catalog` class
- Added `rows_by_key` method to `DataFrame`
- Added set methods to `ListExpr` and `ListNameSpace`
- Added `filter` method to `ListExpr` and `ListNameSpace`
- Added `extra_columns` option to `scan_parquet` method
- Removed `columns` option from `DataFrame` constructor (use `schema` instead)
- Removed `has_header` option from `write_csv` method (use `include_header` instead)

## 0.20.0 (2025-06-23)

- Updated Polars to 0.49.1

## 0.19.0 (2025-05-20)

- Updated Polars to 0.48.0

## 0.18.0 (2025-05-05)

- Updated Polars to 0.47.1

## 0.17.1 (2025-04-12)

- Added support for horizontal concatenation of `LazyFrame`s
- Added `diagonal_relaxed` and `align` strategies to `concat` method
- Added `interpolate_by` method to `Series` and `Expr`
- Added `iter_columns` and `iter_slices` methods to `DataFrame`
- Added `maintain_order` option to `join` method
- Added `collect_schema` method to `DataFrame`
- Added `write_database` method to `DataFrame` (experimental)
- Fixed error with `to_numo` method for `Boolean` series with null values
- Fixed error with `slice` method and negative offset

## 0.17.0 (2025-01-28)

- Updated Polars to 0.46.0
- Changed `write_json` method for `DataFrame` to be row-oriented
- Fixed error with `Series` constructor and `strict: false`

## 0.16.0 (2024-12-29)

- Updated Polars to 0.45.1
- Added support for Ruby 3.4
- Added experimental support for Delta Lake
- Added `by_name` selector
- Added `thread_pool_size` method to `Polars`
- Removed `axis` option from `min`, `max`, `sum`, and `mean` methods (use `*_horizontal` instead)
- Dropped support for Ruby < 3.2

## 0.15.0 (2024-11-20)

- Updated Polars to 0.44.2
- I/O methods no longer require a `URI` object for remote files
- Added support for scanning files from cloud storage
- Added experimental support for Arrow C streams
- Added selectors
- Added `config`, `string_cache`, and `cs` methods to `Polars`
- Added `strict` option to `DataFrame` constructor
- Added `compat_level` option to `write_ipc` and `write_ipc_stream` methods
- Added `name` option to `write_avro` method
- Added support for array of name-type pairs to `schema` option
- Added `cast` method to `DataFrame` and `LazyFrame`
- Added `coalesce` option to `join` and `join_asof` methods
- Added `validate` option to `join` method
- Added `strict` option to `rename` method
- Changed `rechunk` option default from `true` to `false` for `scan_parquet` method
- Fixed `limit` method for `LazyFrame`
- Fixed `read_database` connection leasing for Active Record 7.2
- Removed `get_dummies` method from `Polars` (use `df.to_dummies` instead)
- Removed `to_list` method from `Polars` (use `col(name).list` instead)
- Removed `spearman_rank_corr` method from `Polars` (use `corr(method: "spearman")` instead)
- Removed `pearson_corr` method from `Polars` (use `corr(method: "pearson")` instead)
- Removed `inner_dtype` and `time_unit` methods from `Series`

## 0.14.0 (2024-09-17)

- Updated Polars to 0.43.1
- Fixed `frac` option for `sample` method

## 0.13.0 (2024-09-04)

- Updated Polars to 0.42.0
- Added precompiled gem for Linux ARM MUSL
- Added precompiled gem for Windows

## 0.12.0 (2024-07-11)

- Updated Polars to 0.41.3
- Added `nth` method to `Polars`
- Added `get` method to `Expr`
- Added `check_names` option to `equals` method
- Improved `struct` method
- Aliased `melt` to `unpivot` for `DataFrame` and `LazyFrame`
- Changed signature of `pivot` and `melt` methods
- Changed signature of `date_range` and `date_ranges` methods
- Changed `set_sorted` method to only accept a single column
- Removed `use_earliest` option from `replace_time_zone` and `to_datetime` methods (use `ambiguous` instead)
- Removed `by` and `closed` options from `rolling_*` methods (use `rolling_*_by` instead)
- Removed `explode` method from `StringExpr` (use `split("").explode` instead)
- Removed `set_ordering` method from `CatExpr`

## 0.11.0 (2024-06-02)

- Updated Polars to 0.40.0
- Added `date_ranges` method to `Polars`
- Added `read_ipc_stream` method to `Polars`
- Added `write_ipc_stream` to `DataFrame`
- Added `flags` method to `DataFrame`
- Added support for keyword arguments to `agg` methods
- Aliased `apply` to `map_rows` for `DataFrame`
- Changed default `name` for `with_row_index` from `row_nr` to `index`

## 0.10.0 (2024-05-02)

- Updated Polars to 0.39.2
- Added support for writing JSON to string
- Added support for writing Parquet to `StringIO`
- Added support for cross joins
- Added `data_page_size` option to `write_parquet` method
- Added `truncate_ragged_lines` option to `read_csv`, `read_csv_batched`, and `scan_csv` methods
- Added precompiled gem for Linux x86-64 MUSL
- Changed `drop` method to ignore missing columns
- Fixed error with `then` method

## 0.9.0 (2024-03-03)

See the [upgrade guide](https://docs.pola.rs/releases/upgrade/0.20/)

- Updated Polars to 0.38.1
- Changed `count` method to exclude null values
- Changed `dtype` and `schema` methods to always return instances of data types
- Added `Enum` type
- Added `Testing` module
- Added `arctan2`, `arctan2d`, `set_random_seed`, and `sql_expr` methods to `Polars`
- Added `enable_string_cache`, `disable_string_cache`, and `using_string_cache` to `Polars`
- Added methods for horizontal aggregations to `Polars`
- Added `sink_ipc`, `sink_csv`, and `sink_ndjson` methods to `LazyFrame`
- Added `replace` method to `Series` and `Expr`
- Added `eq`, `eq_missing`, `ne`, and `ne_missing` methods to `Series` and `Expr`
- Added `ge`, `gt`, `le`, and `lt` methods to `Series` and `Expr`
- Added `merge_sorted` method to `DataFrame` and `LazyFrame`
- Added more methods to `ArrayExpr` and `ArrayNameSpace`
- Added more methods to `CatExpr` and `CatNameSpace`
- Added more methods to `ListExpr` and `ListNameSpace`
- Added more methods to `MetaExpr`
- Added more methods to `StringExpr`
- Added `schema_overrides` option to `read_database` method
- Added `join_nulls` option to `join` method
- Added `ignore_nulls` option to `any` and `all` methods
- Aliased `apply` to `map_elements` for `Series`
- Aliased `cleared` to `clear` for `DataFrame` and `LazyFrame`
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

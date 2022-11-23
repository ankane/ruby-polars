mod conversion;
mod dataframe;
mod error;
mod file;
mod lazy;
mod series;

use conversion::get_df;
use dataframe::RbDataFrame;
use error::{RbPolarsErr, RbValueError};
use lazy::dataframe::{RbLazyFrame, RbLazyGroupBy};
use lazy::dsl::{RbExpr, RbWhen, RbWhenThen};
use magnus::{
    define_module, function, memoize, method, prelude::*, Error, RArray, RClass, RModule,
};
use polars::error::PolarsResult;
use polars::frame::DataFrame;
use polars::functions::{diag_concat_df, hor_concat_df};
use series::RbSeries;

type RbResult<T> = Result<T, Error>;

fn module() -> RModule {
    *memoize!(RModule: define_module("Polars").unwrap())
}

fn series() -> RClass {
    *memoize!(RClass: module().define_class("Series", Default::default()).unwrap())
}

#[magnus::init]
fn init() -> RbResult<()> {
    let module = module();
    module.define_singleton_method("_concat_df", function!(concat_df, 1))?;
    module.define_singleton_method("_diag_concat_df", function!(rb_diag_concat_df, 1))?;
    module.define_singleton_method("_hor_concat_df", function!(rb_hor_concat_df, 1))?;

    let class = module.define_class("RbDataFrame", Default::default())?;
    class.define_singleton_method("new", function!(RbDataFrame::init, 1))?;
    class.define_singleton_method("read_csv", function!(RbDataFrame::read_csv, 2))?;
    class.define_singleton_method("read_parquet", function!(RbDataFrame::read_parquet, 1))?;
    class.define_singleton_method("read_hash", function!(RbDataFrame::read_hash, 1))?;
    class.define_singleton_method("read_json", function!(RbDataFrame::read_json, 1))?;
    class.define_singleton_method("read_ndjson", function!(RbDataFrame::read_ndjson, 1))?;
    class.define_method("estimated_size", method!(RbDataFrame::estimated_size, 0))?;
    class.define_method("write_json", method!(RbDataFrame::write_json, 3))?;
    class.define_method("write_ndjson", method!(RbDataFrame::write_ndjson, 1))?;
    class.define_method("write_csv", method!(RbDataFrame::write_csv, 10))?;
    class.define_method("write_parquet", method!(RbDataFrame::write_parquet, 5))?;
    class.define_method("rechunk", method!(RbDataFrame::rechunk, 0))?;
    class.define_method("to_s", method!(RbDataFrame::to_s, 0))?;
    class.define_method("get_columns", method!(RbDataFrame::get_columns, 0))?;
    class.define_method("columns", method!(RbDataFrame::columns, 0))?;
    class.define_method(
        "set_column_names",
        method!(RbDataFrame::set_column_names, 1),
    )?;
    class.define_method("dtypes", method!(RbDataFrame::dtypes, 0))?;
    class.define_method("n_chunks", method!(RbDataFrame::n_chunks, 0))?;
    class.define_method("shape", method!(RbDataFrame::shape, 0))?;
    class.define_method("height", method!(RbDataFrame::height, 0))?;
    class.define_method("width", method!(RbDataFrame::width, 0))?;
    class.define_method("select_at_idx", method!(RbDataFrame::select_at_idx, 1))?;
    class.define_method("column", method!(RbDataFrame::column, 1))?;
    class.define_method("select", method!(RbDataFrame::select, 1))?;
    class.define_method("take", method!(RbDataFrame::take, 1))?;
    class.define_method(
        "take_with_series",
        method!(RbDataFrame::take_with_series, 1),
    )?;
    class.define_method("sort", method!(RbDataFrame::sort, 3))?;
    class.define_method("replace", method!(RbDataFrame::replace, 2))?;
    class.define_method("replace_at_idx", method!(RbDataFrame::replace_at_idx, 2))?;
    class.define_method("insert_at_idx", method!(RbDataFrame::insert_at_idx, 2))?;
    class.define_method("slice", method!(RbDataFrame::slice, 2))?;
    class.define_method("head", method!(RbDataFrame::head, 1))?;
    class.define_method("tail", method!(RbDataFrame::tail, 1))?;
    class.define_method("is_unique", method!(RbDataFrame::is_unique, 0))?;
    class.define_method("is_duplicated", method!(RbDataFrame::is_duplicated, 0))?;
    class.define_method("frame_equal", method!(RbDataFrame::frame_equal, 2))?;
    class.define_method("with_row_count", method!(RbDataFrame::with_row_count, 2))?;
    class.define_method("_clone", method!(RbDataFrame::clone, 0))?;
    class.define_method("melt", method!(RbDataFrame::melt, 4))?;
    class.define_method("partition_by", method!(RbDataFrame::partition_by, 2))?;
    class.define_method("shift", method!(RbDataFrame::shift, 1))?;
    class.define_method("unique", method!(RbDataFrame::unique, 3))?;
    class.define_method("lazy", method!(RbDataFrame::lazy, 0))?;
    class.define_method("max", method!(RbDataFrame::max, 0))?;
    class.define_method("min", method!(RbDataFrame::min, 0))?;
    class.define_method("sum", method!(RbDataFrame::sum, 0))?;
    class.define_method("mean", method!(RbDataFrame::mean, 0))?;
    class.define_method("std", method!(RbDataFrame::std, 1))?;
    class.define_method("var", method!(RbDataFrame::var, 1))?;
    class.define_method("median", method!(RbDataFrame::median, 0))?;
    class.define_method("hmean", method!(RbDataFrame::hmean, 1))?;
    class.define_method("hmax", method!(RbDataFrame::hmax, 0))?;
    class.define_method("hmin", method!(RbDataFrame::hmin, 0))?;
    class.define_method("hsum", method!(RbDataFrame::hsum, 1))?;
    class.define_method("quantile", method!(RbDataFrame::quantile, 2))?;
    class.define_method("to_dummies", method!(RbDataFrame::to_dummies, 1))?;
    class.define_method("null_count", method!(RbDataFrame::null_count, 0))?;
    class.define_method("shrink_to_fit", method!(RbDataFrame::shrink_to_fit, 0))?;
    class.define_method("transpose", method!(RbDataFrame::transpose, 2))?;
    class.define_method("upsample", method!(RbDataFrame::upsample, 5))?;
    class.define_method("unnest", method!(RbDataFrame::unnest, 1))?;

    let class = module.define_class("RbExpr", Default::default())?;
    class.define_method("+", method!(RbExpr::add, 1))?;
    class.define_method("-", method!(RbExpr::sub, 1))?;
    class.define_method("*", method!(RbExpr::mul, 1))?;
    class.define_method("truediv", method!(RbExpr::truediv, 1))?;
    class.define_method("%", method!(RbExpr::_mod, 1))?;
    class.define_method("floordiv", method!(RbExpr::floordiv, 1))?;
    class.define_method("to_str", method!(RbExpr::to_str, 0))?;
    class.define_method("eq", method!(RbExpr::eq, 1))?;
    class.define_method("neq", method!(RbExpr::neq, 1))?;
    class.define_method("gt", method!(RbExpr::gt, 1))?;
    class.define_method("gt_eq", method!(RbExpr::gt_eq, 1))?;
    class.define_method("lt_eq", method!(RbExpr::lt_eq, 1))?;
    class.define_method("lt", method!(RbExpr::lt, 1))?;
    class.define_method("_alias", method!(RbExpr::alias, 1))?;
    class.define_method("is_not", method!(RbExpr::is_not, 0))?;
    class.define_method("is_null", method!(RbExpr::is_null, 0))?;
    class.define_method("is_not_null", method!(RbExpr::is_not_null, 0))?;
    class.define_method("is_infinite", method!(RbExpr::is_infinite, 0))?;
    class.define_method("is_finite", method!(RbExpr::is_finite, 0))?;
    class.define_method("is_nan", method!(RbExpr::is_nan, 0))?;
    class.define_method("is_not_nan", method!(RbExpr::is_not_nan, 0))?;
    class.define_method("min", method!(RbExpr::min, 0))?;
    class.define_method("max", method!(RbExpr::max, 0))?;
    class.define_method("nan_max", method!(RbExpr::nan_max, 0))?;
    class.define_method("nan_min", method!(RbExpr::nan_min, 0))?;
    class.define_method("mean", method!(RbExpr::mean, 0))?;
    class.define_method("median", method!(RbExpr::median, 0))?;
    class.define_method("sum", method!(RbExpr::sum, 0))?;
    class.define_method("n_unique", method!(RbExpr::n_unique, 0))?;
    class.define_method("arg_unique", method!(RbExpr::arg_unique, 0))?;
    class.define_method("unique", method!(RbExpr::unique, 0))?;
    class.define_method("unique_stable", method!(RbExpr::unique_stable, 0))?;
    class.define_method("first", method!(RbExpr::first, 0))?;
    class.define_method("last", method!(RbExpr::last, 0))?;
    class.define_method("list", method!(RbExpr::list, 0))?;
    class.define_method("quantile", method!(RbExpr::quantile, 2))?;
    class.define_method("agg_groups", method!(RbExpr::agg_groups, 0))?;
    class.define_method("count", method!(RbExpr::count, 0))?;
    class.define_method("value_counts", method!(RbExpr::value_counts, 2))?;
    class.define_method("unique_counts", method!(RbExpr::unique_counts, 0))?;
    class.define_method("null_count", method!(RbExpr::null_count, 0))?;
    class.define_method("cast", method!(RbExpr::cast, 2))?;
    class.define_method("sort_with", method!(RbExpr::sort_with, 2))?;
    class.define_method("arg_sort", method!(RbExpr::arg_sort, 2))?;
    class.define_method("top_k", method!(RbExpr::top_k, 2))?;
    class.define_method("arg_max", method!(RbExpr::arg_max, 0))?;
    class.define_method("arg_min", method!(RbExpr::arg_min, 0))?;
    class.define_method("search_sorted", method!(RbExpr::search_sorted, 1))?;
    class.define_method("take", method!(RbExpr::take, 1))?;
    class.define_method("sort_by", method!(RbExpr::sort_by, 2))?;
    class.define_method("backward_fill", method!(RbExpr::backward_fill, 1))?;
    class.define_method("forward_fill", method!(RbExpr::forward_fill, 1))?;
    class.define_method("shift", method!(RbExpr::shift, 1))?;
    class.define_method("shift_and_fill", method!(RbExpr::shift_and_fill, 2))?;
    class.define_method("fill_null", method!(RbExpr::fill_null, 1))?;
    class.define_method(
        "fill_null_with_strategy",
        method!(RbExpr::fill_null_with_strategy, 2),
    )?;
    class.define_method("fill_nan", method!(RbExpr::fill_nan, 1))?;
    class.define_method("drop_nulls", method!(RbExpr::drop_nulls, 0))?;
    class.define_method("drop_nans", method!(RbExpr::drop_nans, 0))?;
    class.define_method("filter", method!(RbExpr::filter, 1))?;
    class.define_method("reverse", method!(RbExpr::reverse, 0))?;
    class.define_method("std", method!(RbExpr::std, 1))?;
    class.define_method("var", method!(RbExpr::var, 1))?;
    class.define_method("is_unique", method!(RbExpr::is_unique, 0))?;
    class.define_method("is_first", method!(RbExpr::is_first, 0))?;
    class.define_method("explode", method!(RbExpr::explode, 0))?;
    class.define_method("take_every", method!(RbExpr::take_every, 1))?;
    class.define_method("tail", method!(RbExpr::tail, 1))?;
    class.define_method("head", method!(RbExpr::head, 1))?;
    class.define_method("slice", method!(RbExpr::slice, 2))?;
    class.define_method("append", method!(RbExpr::append, 2))?;
    class.define_method("rechunk", method!(RbExpr::rechunk, 0))?;
    class.define_method("round", method!(RbExpr::round, 1))?;
    class.define_method("floor", method!(RbExpr::floor, 0))?;
    class.define_method("ceil", method!(RbExpr::ceil, 0))?;
    class.define_method("clip", method!(RbExpr::clip, 2))?;
    class.define_method("clip_min", method!(RbExpr::clip_min, 1))?;
    class.define_method("clip_max", method!(RbExpr::clip_max, 1))?;
    class.define_method("abs", method!(RbExpr::abs, 0))?;
    class.define_method("sin", method!(RbExpr::sin, 0))?;
    class.define_method("cos", method!(RbExpr::cos, 0))?;
    class.define_method("tan", method!(RbExpr::tan, 0))?;
    class.define_method("arcsin", method!(RbExpr::arcsin, 0))?;
    class.define_method("arccos", method!(RbExpr::arccos, 0))?;
    class.define_method("arctan", method!(RbExpr::arctan, 0))?;
    class.define_method("sinh", method!(RbExpr::sinh, 0))?;
    class.define_method("cosh", method!(RbExpr::cosh, 0))?;
    class.define_method("tanh", method!(RbExpr::tanh, 0))?;
    class.define_method("arcsinh", method!(RbExpr::arcsinh, 0))?;
    class.define_method("arccosh", method!(RbExpr::arccosh, 0))?;
    class.define_method("arctanh", method!(RbExpr::arctanh, 0))?;
    class.define_method("sign", method!(RbExpr::sign, 0))?;
    class.define_method("is_duplicated", method!(RbExpr::is_duplicated, 0))?;
    class.define_method("over", method!(RbExpr::over, 1))?;
    class.define_method("_and", method!(RbExpr::_and, 1))?;
    class.define_method("_xor", method!(RbExpr::_xor, 1))?;
    class.define_method("_or", method!(RbExpr::_or, 1))?;
    class.define_method("is_in", method!(RbExpr::is_in, 1))?;
    class.define_method("repeat_by", method!(RbExpr::repeat_by, 1))?;
    class.define_method("pow", method!(RbExpr::pow, 1))?;
    class.define_method("cumsum", method!(RbExpr::cumsum, 1))?;
    class.define_method("cummax", method!(RbExpr::cummax, 1))?;
    class.define_method("cummin", method!(RbExpr::cummin, 1))?;
    class.define_method("cumprod", method!(RbExpr::cumprod, 1))?;
    class.define_method("product", method!(RbExpr::product, 0))?;
    class.define_method("shrink_dtype", method!(RbExpr::shrink_dtype, 0))?;
    class.define_method("str_parse_date", method!(RbExpr::str_parse_date, 3))?;
    class.define_method("str_parse_datetime", method!(RbExpr::str_parse_datetime, 3))?;
    class.define_method("str_parse_time", method!(RbExpr::str_parse_time, 3))?;
    class.define_method("str_strip", method!(RbExpr::str_strip, 1))?;
    class.define_method("str_rstrip", method!(RbExpr::str_rstrip, 1))?;
    class.define_method("str_lstrip", method!(RbExpr::str_lstrip, 1))?;
    class.define_method("str_slice", method!(RbExpr::str_slice, 2))?;
    class.define_method("str_to_uppercase", method!(RbExpr::str_to_uppercase, 0))?;
    class.define_method("str_to_lowercase", method!(RbExpr::str_to_lowercase, 0))?;
    class.define_method("str_lengths", method!(RbExpr::str_lengths, 0))?;
    class.define_method("str_n_chars", method!(RbExpr::str_n_chars, 0))?;
    class.define_method("str_replace", method!(RbExpr::str_replace, 3))?;
    class.define_method("str_replace_all", method!(RbExpr::str_replace_all, 3))?;
    class.define_method("str_contains", method!(RbExpr::str_contains, 2))?;
    class.define_method("str_ends_with", method!(RbExpr::str_ends_with, 1))?;
    class.define_method("str_starts_with", method!(RbExpr::str_starts_with, 1))?;
    class.define_method("str_extract", method!(RbExpr::str_extract, 2))?;
    class.define_method("str_extract_all", method!(RbExpr::str_extract_all, 1))?;
    class.define_method("count_match", method!(RbExpr::count_match, 1))?;
    class.define_method("strftime", method!(RbExpr::strftime, 1))?;
    class.define_method("str_split", method!(RbExpr::str_split, 1))?;
    class.define_method(
        "str_split_inclusive",
        method!(RbExpr::str_split_inclusive, 1),
    )?;
    class.define_method("str_split_exact", method!(RbExpr::str_split_exact, 2))?;
    class.define_method(
        "str_split_exact_inclusive",
        method!(RbExpr::str_split_exact_inclusive, 2),
    )?;
    class.define_method("str_splitn", method!(RbExpr::str_splitn, 2))?;
    class.define_method("arr_lengths", method!(RbExpr::arr_lengths, 0))?;
    class.define_method("arr_contains", method!(RbExpr::arr_contains, 1))?;
    class.define_method("prefix", method!(RbExpr::prefix, 1))?;
    class.define_method("suffix", method!(RbExpr::suffix, 1))?;
    class.define_method("interpolate", method!(RbExpr::interpolate, 0))?;
    class.define_method("any", method!(RbExpr::any, 0))?;
    class.define_method("all", method!(RbExpr::all, 0))?;

    // maybe add to different class
    class.define_singleton_method("col", function!(crate::lazy::dsl::col, 1))?;
    class.define_singleton_method("lit", function!(crate::lazy::dsl::lit, 1))?;
    class.define_singleton_method("arange", function!(crate::lazy::dsl::arange, 3))?;
    class.define_singleton_method("when", function!(crate::lazy::dsl::when, 1))?;

    let class = module.define_class("RbLazyFrame", Default::default())?;
    class.define_method("write_json", method!(RbLazyFrame::write_json, 1))?;
    class.define_method("describe_plan", method!(RbLazyFrame::describe_plan, 0))?;
    class.define_method(
        "describe_optimized_plan",
        method!(RbLazyFrame::describe_optimized_plan, 0),
    )?;
    class.define_method(
        "optimization_toggle",
        method!(RbLazyFrame::optimization_toggle, 7),
    )?;
    class.define_method("sort", method!(RbLazyFrame::sort, 3))?;
    class.define_method("sort_by_exprs", method!(RbLazyFrame::sort_by_exprs, 3))?;
    class.define_method("cache", method!(RbLazyFrame::cache, 0))?;
    class.define_method("collect", method!(RbLazyFrame::collect, 0))?;
    class.define_method("fetch", method!(RbLazyFrame::fetch, 1))?;
    class.define_method("filter", method!(RbLazyFrame::filter, 1))?;
    class.define_method("select", method!(RbLazyFrame::select, 1))?;
    class.define_method("groupby", method!(RbLazyFrame::groupby, 2))?;
    class.define_method("groupby_rolling", method!(RbLazyFrame::groupby_rolling, 5))?;
    class.define_method("groupby_dynamic", method!(RbLazyFrame::groupby_dynamic, 8))?;
    class.define_method("join", method!(RbLazyFrame::join, 7))?;
    class.define_method("with_columns", method!(RbLazyFrame::with_columns, 1))?;
    class.define_method("rename", method!(RbLazyFrame::rename, 2))?;
    class.define_method("reverse", method!(RbLazyFrame::reverse, 0))?;
    class.define_method("shift", method!(RbLazyFrame::shift, 1))?;
    class.define_method("shift_and_fill", method!(RbLazyFrame::shift_and_fill, 2))?;
    class.define_method("fill_nan", method!(RbLazyFrame::fill_nan, 1))?;
    class.define_method("min", method!(RbLazyFrame::min, 0))?;
    class.define_method("max", method!(RbLazyFrame::max, 0))?;
    class.define_method("sum", method!(RbLazyFrame::sum, 0))?;
    class.define_method("mean", method!(RbLazyFrame::mean, 0))?;
    class.define_method("std", method!(RbLazyFrame::std, 1))?;
    class.define_method("var", method!(RbLazyFrame::var, 1))?;
    class.define_method("median", method!(RbLazyFrame::median, 0))?;
    class.define_method("quantile", method!(RbLazyFrame::quantile, 2))?;
    class.define_method("explode", method!(RbLazyFrame::explode, 1))?;
    class.define_method("unique", method!(RbLazyFrame::unique, 3))?;
    class.define_method("drop_nulls", method!(RbLazyFrame::drop_nulls, 1))?;
    class.define_method("slice", method!(RbLazyFrame::slice, 2))?;
    class.define_method("tail", method!(RbLazyFrame::tail, 1))?;
    class.define_method("melt", method!(RbLazyFrame::melt, 4))?;
    class.define_method("with_row_count", method!(RbLazyFrame::with_row_count, 2))?;
    class.define_method("drop_columns", method!(RbLazyFrame::drop_columns, 1))?;
    class.define_method("_clone", method!(RbLazyFrame::clone, 0))?;
    class.define_method("columns", method!(RbLazyFrame::columns, 0))?;
    class.define_method("dtypes", method!(RbLazyFrame::dtypes, 0))?;
    class.define_method("schema", method!(RbLazyFrame::schema, 0))?;
    class.define_method("unnest", method!(RbLazyFrame::unnest, 1))?;
    class.define_method("width", method!(RbLazyFrame::width, 0))?;

    let class = module.define_class("RbLazyGroupBy", Default::default())?;
    class.define_method("agg", method!(RbLazyGroupBy::agg, 1))?;
    class.define_method("head", method!(RbLazyGroupBy::head, 1))?;
    class.define_method("tail", method!(RbLazyGroupBy::tail, 1))?;

    let class = module.define_class("RbSeries", Default::default())?;
    class.define_singleton_method("new_opt_bool", function!(RbSeries::new_opt_bool, 3))?;
    class.define_singleton_method("new_opt_u8", function!(RbSeries::new_opt_u8, 3))?;
    class.define_singleton_method("new_opt_u16", function!(RbSeries::new_opt_u16, 3))?;
    class.define_singleton_method("new_opt_u32", function!(RbSeries::new_opt_u32, 3))?;
    class.define_singleton_method("new_opt_u64", function!(RbSeries::new_opt_u64, 3))?;
    class.define_singleton_method("new_opt_i8", function!(RbSeries::new_opt_i8, 3))?;
    class.define_singleton_method("new_opt_i16", function!(RbSeries::new_opt_i16, 3))?;
    class.define_singleton_method("new_opt_i32", function!(RbSeries::new_opt_i32, 3))?;
    class.define_singleton_method("new_opt_i64", function!(RbSeries::new_opt_i64, 3))?;
    class.define_singleton_method("new_opt_f32", function!(RbSeries::new_opt_f32, 3))?;
    class.define_singleton_method("new_opt_f64", function!(RbSeries::new_opt_f64, 3))?;
    class.define_singleton_method("new_str", function!(RbSeries::new_str, 3))?;
    class.define_method("is_sorted_flag", method!(RbSeries::is_sorted_flag, 0))?;
    class.define_method(
        "is_sorted_reverse_flag",
        method!(RbSeries::is_sorted_reverse_flag, 0),
    )?;
    class.define_method("estimated_size", method!(RbSeries::estimated_size, 0))?;
    class.define_method("get_fmt", method!(RbSeries::get_fmt, 2))?;
    class.define_method("rechunk", method!(RbSeries::rechunk, 1))?;
    class.define_method("get_idx", method!(RbSeries::get_idx, 1))?;
    class.define_method("bitand", method!(RbSeries::bitand, 1))?;
    class.define_method("bitor", method!(RbSeries::bitor, 1))?;
    class.define_method("bitxor", method!(RbSeries::bitxor, 1))?;
    class.define_method("chunk_lengths", method!(RbSeries::chunk_lengths, 0))?;
    class.define_method("name", method!(RbSeries::name, 0))?;
    class.define_method("rename", method!(RbSeries::rename, 1))?;
    class.define_method("dtype", method!(RbSeries::dtype, 0))?;
    class.define_method("inner_dtype", method!(RbSeries::inner_dtype, 0))?;
    class.define_method("set_sorted", method!(RbSeries::set_sorted, 1))?;
    class.define_method("mean", method!(RbSeries::mean, 0))?;
    class.define_method("max", method!(RbSeries::max, 0))?;
    class.define_method("min", method!(RbSeries::min, 0))?;
    class.define_method("sum", method!(RbSeries::sum, 0))?;
    class.define_method("n_chunks", method!(RbSeries::n_chunks, 0))?;
    class.define_method("append", method!(RbSeries::append, 1))?;
    class.define_method("extend", method!(RbSeries::extend, 1))?;
    class.define_method("new_from_index", method!(RbSeries::new_from_index, 2))?;
    class.define_method("filter", method!(RbSeries::filter, 1))?;
    class.define_method("add", method!(RbSeries::add, 1))?;
    class.define_method("sub", method!(RbSeries::sub, 1))?;
    class.define_method("mul", method!(RbSeries::mul, 1))?;
    class.define_method("div", method!(RbSeries::div, 1))?;
    class.define_method("rem", method!(RbSeries::rem, 1))?;
    class.define_method("sort", method!(RbSeries::sort, 1))?;
    class.define_method("value_counts", method!(RbSeries::value_counts, 1))?;
    class.define_method("arg_min", method!(RbSeries::arg_min, 0))?;
    class.define_method("arg_max", method!(RbSeries::arg_max, 0))?;
    class.define_method("take_with_series", method!(RbSeries::take_with_series, 1))?;
    class.define_method("null_count", method!(RbSeries::null_count, 0))?;
    class.define_method("has_validity", method!(RbSeries::has_validity, 0))?;
    class.define_method("sample_n", method!(RbSeries::sample_n, 4))?;
    class.define_method("sample_frac", method!(RbSeries::sample_frac, 4))?;
    class.define_method("series_equal", method!(RbSeries::series_equal, 3))?;
    class.define_method("eq", method!(RbSeries::eq, 1))?;
    class.define_method("neq", method!(RbSeries::neq, 1))?;
    class.define_method("gt", method!(RbSeries::gt, 1))?;
    class.define_method("gt_eq", method!(RbSeries::gt_eq, 1))?;
    class.define_method("lt", method!(RbSeries::lt, 1))?;
    class.define_method("lt_eq", method!(RbSeries::lt_eq, 1))?;
    class.define_method("not", method!(RbSeries::not, 0))?;
    class.define_method("to_s", method!(RbSeries::to_s, 0))?;
    class.define_method("len", method!(RbSeries::len, 0))?;
    class.define_method("to_a", method!(RbSeries::to_a, 0))?;
    class.define_method("median", method!(RbSeries::median, 0))?;
    class.define_method("quantile", method!(RbSeries::quantile, 2))?;
    class.define_method("_clone", method!(RbSeries::clone, 0))?;
    class.define_method("zip_with", method!(RbSeries::zip_with, 2))?;
    class.define_method("to_dummies", method!(RbSeries::to_dummies, 0))?;
    class.define_method("peak_max", method!(RbSeries::peak_max, 0))?;
    class.define_method("peak_min", method!(RbSeries::peak_min, 0))?;
    class.define_method("n_unique", method!(RbSeries::n_unique, 0))?;
    class.define_method("floor", method!(RbSeries::floor, 0))?;
    class.define_method("shrink_to_fit", method!(RbSeries::shrink_to_fit, 0))?;
    class.define_method("dot", method!(RbSeries::dot, 1))?;
    class.define_method("skew", method!(RbSeries::skew, 1))?;
    class.define_method("kurtosis", method!(RbSeries::kurtosis, 2))?;
    class.define_method("cast", method!(RbSeries::cast, 2))?;
    class.define_method("time_unit", method!(RbSeries::time_unit, 0))?;
    // rest
    class.define_method("cumsum", method!(RbSeries::cumsum, 1))?;
    class.define_method("cummax", method!(RbSeries::cummax, 1))?;
    class.define_method("cummin", method!(RbSeries::cummin, 1))?;
    class.define_method("cumprod", method!(RbSeries::cumprod, 1))?;
    class.define_method("slice", method!(RbSeries::slice, 2))?;
    class.define_method("ceil", method!(RbSeries::ceil, 0))?;
    class.define_method("round", method!(RbSeries::round, 1))?;

    let class = module.define_class("RbWhen", Default::default())?;
    class.define_method("_then", method!(RbWhen::then, 1))?;

    let class = module.define_class("RbWhenThen", Default::default())?;
    class.define_method("otherwise", method!(RbWhenThen::overwise, 1))?;

    Ok(())
}

fn concat_df(seq: RArray) -> RbResult<RbDataFrame> {
    let mut iter = seq.each();
    let first = iter.next().unwrap()?;

    let first_rdf = get_df(first)?;
    let identity_df = first_rdf.slice(0, 0);

    let mut rdfs: Vec<PolarsResult<DataFrame>> = vec![Ok(first_rdf)];

    for item in iter {
        let rdf = get_df(item?)?;
        rdfs.push(Ok(rdf));
    }

    let identity = Ok(identity_df);

    let df = rdfs
        .into_iter()
        .fold(identity, |acc: PolarsResult<DataFrame>, df| {
            let mut acc = acc?;
            acc.vstack_mut(&df?)?;
            Ok(acc)
        })
        .map_err(RbPolarsErr::from)?;

    Ok(df.into())
}

fn rb_diag_concat_df(seq: RArray) -> RbResult<RbDataFrame> {
    let mut dfs = Vec::new();
    for item in seq.each() {
        dfs.push(get_df(item?)?);
    }
    let df = diag_concat_df(&dfs).map_err(RbPolarsErr::from)?;
    Ok(df.into())
}

fn rb_hor_concat_df(seq: RArray) -> RbResult<RbDataFrame> {
    let mut dfs = Vec::new();
    for item in seq.each() {
        dfs.push(get_df(item?)?);
    }
    let df = hor_concat_df(&dfs).map_err(RbPolarsErr::from)?;
    Ok(df.into())
}

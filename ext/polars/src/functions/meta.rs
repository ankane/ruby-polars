use magnus::{IntoValue, Value};
use polars_core;
use polars_core::fmt::FloatFmt;
use polars_core::prelude::IDX_DTYPE;
use polars_core::POOL;

use crate::conversion::Wrap;
use crate::{RbResult, RbValueError};

pub fn get_index_type() -> Value {
    Wrap(IDX_DTYPE).into_value()
}

pub fn threadpool_size() -> usize {
    POOL.current_num_threads()
}

pub fn set_float_fmt(fmt: String) -> RbResult<()> {
    let fmt = match fmt.as_str() {
        "full" => FloatFmt::Full,
        "mixed" => FloatFmt::Mixed,
        e => {
            return Err(RbValueError::new_err(format!(
                "fmt must be one of {{'full', 'mixed'}}, got {e}",
            )))
        }
    };
    polars_core::fmt::set_float_fmt(fmt);
    Ok(())
}

pub fn get_float_fmt() -> RbResult<String> {
    let strfmt = match polars_core::fmt::get_float_fmt() {
        FloatFmt::Full => "full",
        FloatFmt::Mixed => "mixed",
    };
    Ok(strfmt.to_string())
}

pub fn set_float_precision(precision: Option<usize>) -> RbResult<()> {
    use polars_core::fmt::set_float_precision;
    set_float_precision(precision);
    Ok(())
}

pub fn get_float_precision() -> RbResult<Option<usize>> {
    use polars_core::fmt::get_float_precision;
    Ok(get_float_precision())
}

pub fn set_thousands_separator(sep: Option<char>) -> RbResult<()> {
    use polars_core::fmt::set_thousands_separator;
    set_thousands_separator(sep);
    Ok(())
}

pub fn get_thousands_separator() -> RbResult<Option<String>> {
    use polars_core::fmt::get_thousands_separator;
    Ok(Some(get_thousands_separator()))
}

pub fn set_decimal_separator(sep: Option<char>) -> RbResult<()> {
    use polars_core::fmt::set_decimal_separator;
    set_decimal_separator(sep);
    Ok(())
}

pub fn get_decimal_separator() -> RbResult<Option<char>> {
    use polars_core::fmt::get_decimal_separator;
    Ok(Some(get_decimal_separator()))
}

pub fn set_trim_decimal_zeros(trim: Option<bool>) -> RbResult<()> {
    use polars_core::fmt::set_trim_decimal_zeros;
    set_trim_decimal_zeros(trim);
    Ok(())
}

pub fn get_trim_decimal_zeros() -> RbResult<Option<bool>> {
    use polars_core::fmt::get_trim_decimal_zeros;
    Ok(Some(get_trim_decimal_zeros()))
}

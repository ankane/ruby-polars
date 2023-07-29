use magnus::{IntoValue, Value};
use polars_core;
use polars_core::fmt::FloatFmt;
use polars_core::prelude::IDX_DTYPE;
use polars_core::POOL;

use crate::conversion::Wrap;
use crate::{RbResult, RbValueError};

pub fn get_idx_type() -> Value {
    Wrap(IDX_DTYPE).into_value()
}

pub fn threadpool_size() -> usize {
    POOL.current_num_threads()
}

pub fn enable_string_cache(toggle: bool) {
    polars_core::enable_string_cache(toggle)
}

pub fn using_string_cache() -> bool {
    polars_core::using_string_cache()
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

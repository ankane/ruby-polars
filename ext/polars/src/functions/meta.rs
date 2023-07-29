use magnus::{IntoValue, Value};
use polars_core;
use polars_core::fmt::FloatFmt;
use polars_core::prelude::IDX_DTYPE;

use crate::conversion::Wrap;
use crate::{RbResult, RbValueError};

pub fn get_idx_type() -> Value {
    Wrap(IDX_DTYPE).into_value()
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

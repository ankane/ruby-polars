use magnus::{IntoValue, Value};
use polars_core::prelude::IDX_DTYPE;

use crate::conversion::Wrap;

pub fn get_idx_type() -> Value {
    Wrap(IDX_DTYPE).into_value()
}

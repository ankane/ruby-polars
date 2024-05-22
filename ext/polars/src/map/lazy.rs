use magnus::Value;
use polars::prelude::*;

use crate::{RbExpr, Wrap};

pub fn binary_lambda(_lambda: Value, _a: Series, _b: Series) -> PolarsResult<Option<Series>> {
    todo!();
}

pub fn map_single(
    _rbexpr: &RbExpr,
    _lambda: Value,
    _output_type: Option<Wrap<DataType>>,
    _agg_list: bool,
    _is_elementwise: bool,
) -> RbExpr {
    todo!();
}

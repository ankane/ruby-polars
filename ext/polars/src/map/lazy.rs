use magnus::Value;
use polars::prelude::*;

use crate::{RbExpr, Wrap};

pub fn binary_lambda(_lambda: Value, _a: Series, _b: Series) -> PolarsResult<Option<Series>> {
    todo!();
}

pub fn map_single(
    rbexpr: &RbExpr,
    _lambda: Value,
    output_type: Option<Wrap<DataType>>,
    agg_list: bool,
    is_elementwise: bool,
) -> RbExpr {
    let output_type = output_type.map(|wrap| wrap.0);

    let output_type2 = output_type.clone();
    let function = move |_s: Series| {
        let _output_type = output_type2.clone().unwrap_or(DataType::Unknown);

        todo!();
    };

    let output_map = GetOutput::map_field(move |fld| match output_type {
        Some(ref dt) => Field::new(fld.name(), dt.clone()),
        None => {
            let mut fld = fld.clone();
            fld.coerce(DataType::Unknown);
            fld
        }
    });
    if agg_list {
        rbexpr.clone().inner.map_list(function, output_map).into()
    } else {
        rbexpr.clone().inner.map(function, output_map).into()
    }
}

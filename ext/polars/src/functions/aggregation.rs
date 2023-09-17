use magnus::RArray;
use crate::rb_exprs_to_exprs;
use crate::{RbExpr, RbResult};

pub fn sum_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(polars::lazy::dsl::sum_horizontal(exprs).into())
}

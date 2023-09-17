use magnus::RArray;
use polars::lazy::dsl;

use crate::rb_exprs_to_exprs;
use crate::{RbExpr, RbResult};

pub fn all_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::all_horizontal(exprs).into())
}

pub fn any_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::any_horizontal(exprs).into())
}

pub fn max_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::max_horizontal(exprs).into())
}

pub fn min_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::min_horizontal(exprs).into())
}

pub fn sum_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::sum_horizontal(exprs).into())
}

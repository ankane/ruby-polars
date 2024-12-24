use magnus::RArray;
use polars::lazy::dsl;

use crate::rb_exprs_to_exprs;
use crate::{RbExpr, RbPolarsErr, RbResult};

pub fn all_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let e = dsl::all_horizontal(exprs).map_err(RbPolarsErr::from)?;
    Ok(e.into())
}

pub fn any_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let e = dsl::any_horizontal(exprs).map_err(RbPolarsErr::from)?;
    Ok(e.into())
}

pub fn max_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let e = dsl::max_horizontal(exprs).map_err(RbPolarsErr::from)?;
    Ok(e.into())
}

pub fn min_horizontal(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let e = dsl::min_horizontal(exprs).map_err(RbPolarsErr::from)?;
    Ok(e.into())
}

pub fn sum_horizontal(exprs: RArray, ignore_nulls: bool) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let e = dsl::sum_horizontal(exprs, ignore_nulls).map_err(RbPolarsErr::from)?;
    Ok(e.into())
}

pub fn mean_horizontal(exprs: RArray, ignore_nulls: bool) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let e = dsl::mean_horizontal(exprs, ignore_nulls).map_err(RbPolarsErr::from)?;
    Ok(e.into())
}

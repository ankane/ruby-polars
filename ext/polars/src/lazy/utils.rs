use magnus::RArray;
use polars::lazy::dsl::Expr;

use crate::{RbExpr, RbResult};

pub fn rb_exprs_to_exprs(rb_exprs: RArray) -> RbResult<Vec<Expr>> {
    let mut exprs = Vec::new();
    for item in rb_exprs.each() {
        exprs.push(item?.try_convert::<&RbExpr>()?.inner.clone());
    }
    Ok(exprs)
}

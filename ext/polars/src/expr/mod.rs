mod array;
mod binary;
mod categorical;
pub mod datatype;
mod datetime;
mod general;
mod list;
mod meta;
mod name;
mod rolling;
pub mod selector;
mod string;
mod r#struct;

use magnus::{RArray, prelude::*};
use polars::lazy::dsl::Expr;

use crate::RbResult;

#[magnus::wrap(class = "Polars::RbExpr")]
#[derive(Clone)]
pub struct RbExpr {
    pub inner: Expr,
}

impl From<Expr> for RbExpr {
    fn from(inner: Expr) -> Self {
        RbExpr { inner }
    }
}

pub fn rb_exprs_to_exprs(rb_exprs: RArray) -> RbResult<Vec<Expr>> {
    let mut exprs = Vec::new();
    for item in rb_exprs.into_iter() {
        exprs.push(<&RbExpr>::try_convert(item)?.inner.clone());
    }
    Ok(exprs)
}

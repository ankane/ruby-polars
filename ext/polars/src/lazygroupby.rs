use magnus::RArray;
use polars::lazy::frame::LazyGroupBy;
use std::cell::RefCell;

use crate::expr::rb_exprs_to_exprs;
use crate::{RbLazyFrame, RbResult};

#[magnus::wrap(class = "Polars::RbLazyGroupBy")]
pub struct RbLazyGroupBy {
    pub lgb: RefCell<Option<LazyGroupBy>>,
}

impl RbLazyGroupBy {
    pub fn agg(&self, aggs: RArray) -> RbResult<RbLazyFrame> {
        let lgb = self.lgb.borrow_mut().take().unwrap();
        let aggs = rb_exprs_to_exprs(aggs)?;
        Ok(lgb.agg(aggs).into())
    }

    pub fn head(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.head(Some(n)).into()
    }

    pub fn tail(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.tail(Some(n)).into()
    }
}

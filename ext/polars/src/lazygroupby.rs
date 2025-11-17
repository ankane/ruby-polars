use magnus::RArray;
use polars::lazy::frame::LazyGroupBy;

use crate::expr::ToExprs;
use crate::{RbLazyFrame, RbResult};

#[magnus::wrap(class = "Polars::RbLazyGroupBy")]
pub struct RbLazyGroupBy {
    pub lgb: Option<LazyGroupBy>,
}

impl RbLazyGroupBy {
    pub fn agg(&self, aggs: RArray) -> RbResult<RbLazyFrame> {
        let lgb = self.lgb.clone().unwrap();
        let aggs = aggs.to_exprs()?;
        Ok(lgb.agg(aggs).into())
    }

    pub fn head(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.clone().unwrap();
        lgb.head(Some(n)).into()
    }

    pub fn tail(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.clone().unwrap();
        lgb.tail(Some(n)).into()
    }
}

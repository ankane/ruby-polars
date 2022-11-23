use magnus::RArray;
use polars::lazy::frame::{LazyFrame, LazyGroupBy};
use std::cell::RefCell;

use crate::conversion::wrap_join_type;
use crate::lazy::utils::rb_exprs_to_exprs;
use crate::{RbDataFrame, RbExpr, RbPolarsErr, RbResult};

#[magnus::wrap(class = "Polars::RbLazyGroupBy")]
pub struct RbLazyGroupBy {
    lgb: RefCell<Option<LazyGroupBy>>,
}

impl RbLazyGroupBy {
    pub fn agg(&self, aggs: RArray) -> RbResult<RbLazyFrame> {
        let lgb = self.lgb.borrow_mut().take().unwrap();
        let aggs = rb_exprs_to_exprs(aggs)?;
        Ok(lgb.agg(aggs).into())
    }
}

#[magnus::wrap(class = "Polars::RbLazyFrame")]
#[derive(Clone)]
pub struct RbLazyFrame {
    pub ldf: LazyFrame,
}

impl From<LazyFrame> for RbLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        RbLazyFrame { ldf }
    }
}

impl RbLazyFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn optimization_toggle(
        &self,
        type_coercion: bool,
        predicate_pushdown: bool,
        projection_pushdown: bool,
        simplify_expr: bool,
        slice_pushdown: bool,
        _cse: bool,
        allow_streaming: bool,
    ) -> RbLazyFrame {
        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            // .with_common_subplan_elimination(cse)
            .with_streaming(allow_streaming)
            .with_projection_pushdown(projection_pushdown);
        ldf.into()
    }

    pub fn collect(&self) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.collect().map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn filter(&self, predicate: &RbExpr) -> RbLazyFrame {
        let ldf = self.ldf.clone();
        ldf.filter(predicate.inner.clone()).into()
    }

    pub fn select(&self, exprs: RArray) -> RbResult<RbLazyFrame> {
        let ldf = self.ldf.clone();
        let exprs = rb_exprs_to_exprs(exprs)?;
        Ok(ldf.select(exprs).into())
    }

    pub fn groupby(&self, by: RArray, maintain_order: bool) -> RbResult<RbLazyGroupBy> {
        let ldf = self.ldf.clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = if maintain_order {
            ldf.groupby_stable(by)
        } else {
            ldf.groupby(by)
        };
        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join(
        &self,
        other: &RbLazyFrame,
        left_on: RArray,
        right_on: RArray,
        allow_parallel: bool,
        force_parallel: bool,
        how: String,
        suffix: String,
    ) -> RbResult<Self> {
        let how = wrap_join_type(&how)?;

        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = rb_exprs_to_exprs(left_on)?;
        let right_on = rb_exprs_to_exprs(right_on)?;

        Ok(ldf
            .join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(how)
            .suffix(suffix)
            .finish()
            .into())
    }

    pub fn with_columns(&self, exprs: RArray) -> RbResult<RbLazyFrame> {
        let ldf = self.ldf.clone();
        Ok(ldf.with_columns(rb_exprs_to_exprs(exprs)?).into())
    }

    pub fn rename(&self, existing: Vec<String>, new: Vec<String>) -> RbLazyFrame {
        let ldf = self.ldf.clone();
        ldf.rename(existing, new).into()
    }

    pub fn reverse(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.reverse().into()
    }

    pub fn shift(&self, periods: i64) -> Self {
        let ldf = self.ldf.clone();
        ldf.shift(periods).into()
    }

    pub fn shift_and_fill(&self, periods: i64, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.shift_and_fill(periods, fill_value.inner.clone()).into()
    }

    pub fn fill_nan(&self, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.fill_nan(fill_value.inner.clone()).into()
    }

    pub fn min(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.min().into()
    }

    pub fn max(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.max().into()
    }

    pub fn sum(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.sum().into()
    }

    pub fn mean(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.mean().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        let ldf = self.ldf.clone();
        ldf.std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        let ldf = self.ldf.clone();
        ldf.var(ddof).into()
    }

    pub fn median(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.median().into()
    }
}

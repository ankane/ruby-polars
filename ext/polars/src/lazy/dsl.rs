use magnus::{class, RArray, RString, Value};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::apply::lazy::*;
use crate::lazy::utils::rb_exprs_to_exprs;
use crate::{RbExpr, RbPolarsErr, RbResult, RbSeries};

pub fn col(name: String) -> RbExpr {
    dsl::col(&name).into()
}

pub fn count() -> RbExpr {
    dsl::count().into()
}

pub fn first() -> RbExpr {
    dsl::first().into()
}

pub fn last() -> RbExpr {
    dsl::last().into()
}

pub fn cols(names: Vec<String>) -> RbExpr {
    dsl::cols(names).into()
}

pub fn dtype_cols(dtypes: Vec<DataType>) -> RbExpr {
    dsl::dtype_cols(dtypes).into()
}

pub fn fold(acc: &RbExpr, lambda: Value, exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;

    let func = move |a: Series, b: Series| binary_lambda(lambda, a, b);
    Ok(polars::lazy::dsl::fold_exprs(acc.inner.clone(), func, exprs).into())
}

pub fn cumfold(acc: &RbExpr, lambda: Value, exprs: RArray, include_init: bool) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;

    let func = move |a: Series, b: Series| binary_lambda(lambda, a, b);
    Ok(polars::lazy::dsl::cumfold_exprs(acc.inner.clone(), func, exprs, include_init).into())
}

// TODO improve
pub fn lit(value: Value) -> RbResult<RbExpr> {
    if value.is_nil() {
        Ok(dsl::lit(Null {}).into())
    } else if let Ok(series) = value.try_convert::<&RbSeries>() {
        Ok(dsl::lit(series.series.borrow().clone()).into())
    } else if let Some(v) = RString::from_value(value) {
        Ok(dsl::lit(v.try_convert::<String>()?).into())
    } else if value.is_kind_of(class::integer()) {
        match value.try_convert::<i64>() {
            Ok(val) => {
                if val > 0 && val < i32::MAX as i64 || val < 0 && val > i32::MIN as i64 {
                    Ok(dsl::lit(val as i32).into())
                } else {
                    Ok(dsl::lit(val).into())
                }
            }
            _ => {
                let val = value.try_convert::<u64>()?;
                Ok(dsl::lit(val).into())
            }
        }
    } else {
        Ok(dsl::lit(value.try_convert::<f64>()?).into())
    }
}

pub fn repeat(value: Value, n_times: &RbExpr) -> RbResult<RbExpr> {
    if value.is_nil() {
        Ok(polars::lazy::dsl::repeat(Null {}, n_times.inner.clone()).into())
    } else {
        todo!();
    }
}

pub fn pearson_corr(a: &RbExpr, b: &RbExpr, ddof: u8) -> RbExpr {
    polars::lazy::dsl::pearson_corr(a.inner.clone(), b.inner.clone(), ddof).into()
}

pub fn spearman_rank_corr(a: &RbExpr, b: &RbExpr, ddof: u8, propagate_nans: bool) -> RbExpr {
    polars::lazy::dsl::spearman_rank_corr(a.inner.clone(), b.inner.clone(), ddof, propagate_nans)
        .into()
}

pub fn cov(a: &RbExpr, b: &RbExpr) -> RbExpr {
    polars::lazy::dsl::cov(a.inner.clone(), b.inner.clone()).into()
}

#[magnus::wrap(class = "Polars::RbWhen")]
#[derive(Clone)]
pub struct RbWhen {
    pub inner: dsl::When,
}

impl From<dsl::When> for RbWhen {
    fn from(inner: dsl::When) -> Self {
        RbWhen { inner }
    }
}

#[magnus::wrap(class = "Polars::RbWhenThen")]
#[derive(Clone)]
pub struct RbWhenThen {
    pub inner: dsl::WhenThen,
}

impl From<dsl::WhenThen> for RbWhenThen {
    fn from(inner: dsl::WhenThen) -> Self {
        RbWhenThen { inner }
    }
}

impl RbWhen {
    pub fn then(&self, expr: &RbExpr) -> RbWhenThen {
        self.inner.clone().then(expr.inner.clone()).into()
    }
}

impl RbWhenThen {
    pub fn overwise(&self, expr: &RbExpr) -> RbExpr {
        self.inner.clone().otherwise(expr.inner.clone()).into()
    }
}

pub fn when(predicate: &RbExpr) -> RbWhen {
    dsl::when(predicate.inner.clone()).into()
}

pub fn concat_str(s: RArray, sep: String) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    Ok(dsl::concat_str(s, &sep).into())
}

pub fn concat_lst(s: RArray) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    let expr = dsl::concat_list(s).map_err(RbPolarsErr::from)?;
    Ok(expr.into())
}

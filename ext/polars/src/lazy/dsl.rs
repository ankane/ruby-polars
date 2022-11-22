use magnus::{RArray, RString, Value};
use polars::chunked_array::ops::SortOptions;
use polars::lazy::dsl;
use polars::lazy::dsl::Operator;
use polars::prelude::*;

use crate::conversion::parse_fill_null_strategy;
use crate::lazy::utils::rb_exprs_to_exprs;
use crate::RbResult;

#[magnus::wrap(class = "Polars::RbExpr")]
#[derive(Clone)]
pub struct RbExpr {
    pub inner: dsl::Expr,
}

impl From<dsl::Expr> for RbExpr {
    fn from(inner: dsl::Expr) -> Self {
        RbExpr { inner }
    }
}

impl RbExpr {
    pub fn mul(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into())
    }

    pub fn to_str(&self) -> String {
        format!("{:?}", self.inner)
    }

    pub fn eq(&self, other: &RbExpr) -> Self {
        self.clone().inner.eq(other.inner.clone()).into()
    }

    pub fn neq(&self, other: &RbExpr) -> Self {
        self.clone().inner.neq(other.inner.clone()).into()
    }

    pub fn gt(&self, other: &RbExpr) -> Self {
        self.clone().inner.gt(other.inner.clone()).into()
    }

    pub fn gt_eq(&self, other: &RbExpr) -> Self {
        self.clone().inner.gt_eq(other.inner.clone()).into()
    }

    pub fn lt_eq(&self, other: &RbExpr) -> Self {
        self.clone().inner.lt_eq(other.inner.clone()).into()
    }

    pub fn lt(&self, other: &RbExpr) -> Self {
        self.clone().inner.lt(other.inner.clone()).into()
    }

    pub fn alias(&self, name: String) -> Self {
        self.clone().inner.alias(&name).into()
    }

    pub fn is_not(&self) -> Self {
        self.clone().inner.not().into()
    }

    pub fn is_null(&self) -> Self {
        self.clone().inner.is_null().into()
    }

    pub fn is_not_null(&self) -> Self {
        self.clone().inner.is_not_null().into()
    }

    pub fn min(&self) -> Self {
        self.clone().inner.min().into()
    }

    pub fn max(&self) -> Self {
        self.clone().inner.max().into()
    }

    pub fn mean(&self) -> Self {
        self.clone().inner.mean().into()
    }

    pub fn median(&self) -> Self {
        self.clone().inner.median().into()
    }

    pub fn sum(&self) -> Self {
        self.clone().inner.sum().into()
    }

    pub fn n_unique(&self) -> Self {
        self.clone().inner.n_unique().into()
    }

    pub fn unique(&self) -> Self {
        self.clone().inner.unique().into()
    }

    pub fn unique_stable(&self) -> Self {
        self.clone().inner.unique_stable().into()
    }

    pub fn first(&self) -> Self {
        self.clone().inner.first().into()
    }
    pub fn last(&self) -> Self {
        self.clone().inner.last().into()
    }

    pub fn list(&self) -> Self {
        self.clone().inner.list().into()
    }

    pub fn count(&self) -> Self {
        self.clone().inner.count().into()
    }

    pub fn sort_with(&self, descending: bool, nulls_last: bool) -> Self {
        self.clone()
            .inner
            .sort_with(SortOptions {
                descending,
                nulls_last,
            })
            .into()
    }

    pub fn sort_by(&self, by: RArray, reverse: Vec<bool>) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self.clone().inner.sort_by(by, reverse).into())
    }

    pub fn fill_null(&self, expr: &RbExpr) -> Self {
        self.clone().inner.fill_null(expr.inner.clone()).into()
    }

    pub fn fill_null_with_strategy(
        &self,
        strategy: String,
        limit: FillNullLimit,
    ) -> RbResult<Self> {
        let strat = parse_fill_null_strategy(&strategy, limit)?;
        Ok(self
            .inner
            .clone()
            .apply(move |s| s.fill_null(strat), GetOutput::same_type())
            .with_fmt("fill_null_with_strategy")
            .into())
    }

    pub fn fill_nan(&self, expr: &RbExpr) -> Self {
        self.inner.clone().fill_nan(expr.inner.clone()).into()
    }

    pub fn drop_nulls(&self) -> Self {
        self.inner.clone().drop_nulls().into()
    }

    pub fn drop_nans(&self) -> Self {
        self.inner.clone().drop_nans().into()
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        self.clone().inner.filter(predicate.inner.clone()).into()
    }

    pub fn reverse(&self) -> Self {
        self.clone().inner.reverse().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        self.clone().inner.std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        self.clone().inner.var(ddof).into()
    }

    pub fn tail(&self, n: Option<usize>) -> Self {
        self.clone().inner.tail(n).into()
    }

    pub fn head(&self, n: Option<usize>) -> Self {
        self.clone().inner.head(n).into()
    }

    pub fn over(&self, partition_by: RArray) -> RbResult<Self> {
        let partition_by = rb_exprs_to_exprs(partition_by)?;
        Ok(self.clone().inner.over(partition_by).into())
    }

    pub fn _and(&self, expr: &RbExpr) -> Self {
        self.clone().inner.and(expr.inner.clone()).into()
    }

    pub fn _xor(&self, expr: &RbExpr) -> Self {
        self.clone().inner.xor(expr.inner.clone()).into()
    }

    pub fn _or(&self, expr: &RbExpr) -> Self {
        self.clone().inner.or(expr.inner.clone()).into()
    }

    pub fn pow(&self, exponent: &RbExpr) -> Self {
        self.clone().inner.pow(exponent.inner.clone()).into()
    }

    pub fn product(&self) -> Self {
        self.clone().inner.product().into()
    }

    pub fn str_lengths(&self) -> RbExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(ca.str_lengths().into_series())
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.lengths")
            .into()
    }

    pub fn str_contains(&self, pat: String, literal: Option<bool>) -> Self {
        match literal {
            Some(true) => self.inner.clone().str().contains_literal(pat).into(),
            _ => self.inner.clone().str().contains(pat).into(),
        }
    }

    pub fn prefix(&self, prefix: String) -> RbExpr {
        self.inner.clone().prefix(&prefix).into()
    }

    pub fn suffix(&self, suffix: String) -> RbExpr {
        self.inner.clone().suffix(&suffix).into()
    }

    pub fn interpolate(&self) -> RbExpr {
        self.inner.clone().interpolate().into()
    }

    pub fn any(&self) -> Self {
        self.inner.clone().any().into()
    }

    pub fn all(&self) -> Self {
        self.inner.clone().all().into()
    }
}

pub fn col(name: String) -> RbExpr {
    dsl::col(&name).into()
}

// TODO improve
pub fn lit(value: Value) -> RbResult<RbExpr> {
    if value.is_nil() {
        Ok(dsl::lit(Null {}).into())
    } else if let Some(v) = RString::from_value(value) {
        Ok(dsl::lit(v.try_convert::<String>()?).into())
    } else {
        Ok(dsl::lit(value.try_convert::<f64>()?).into())
    }
}

pub fn arange(low: &RbExpr, high: &RbExpr, step: usize) -> RbExpr {
    polars::lazy::dsl::arange(low.inner.clone(), high.inner.clone(), step).into()
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

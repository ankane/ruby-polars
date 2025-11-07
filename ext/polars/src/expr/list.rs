use magnus::{RArray, prelude::*};
use polars::lazy::dsl::lit;
use polars::prelude::*;
use polars::series::ops::NullBehavior;

use crate::conversion::Wrap;
use crate::{RbExpr, RbResult};

impl RbExpr {
    pub fn list_all(&self) -> Self {
        self.inner.clone().list().all().into()
    }

    pub fn list_any(&self) -> Self {
        self.inner.clone().list().any().into()
    }

    pub fn list_arg_max(&self) -> Self {
        self.inner.clone().list().arg_max().into()
    }

    pub fn list_arg_min(&self) -> Self {
        self.inner.clone().list().arg_min().into()
    }

    pub fn list_contains(&self, other: &RbExpr, nulls_equal: bool) -> Self {
        self.inner
            .clone()
            .list()
            .contains(other.inner.clone(), nulls_equal)
            .into()
    }

    pub fn list_count_matches(&self, expr: &RbExpr) -> Self {
        self.inner
            .clone()
            .list()
            .count_matches(expr.inner.clone())
            .into()
    }

    pub fn list_diff(&self, n: i64, null_behavior: Wrap<NullBehavior>) -> RbResult<Self> {
        Ok(self.inner.clone().list().diff(n, null_behavior.0).into())
    }

    pub fn list_eval(&self, expr: &RbExpr) -> Self {
        self.inner.clone().list().eval(expr.inner.clone()).into()
    }

    pub fn list_agg(&self, expr: &RbExpr) -> Self {
        self.inner.clone().list().agg(expr.inner.clone()).into()
    }

    pub fn list_filter(&self, predicate: &RbExpr) -> Self {
        self.inner
            .clone()
            .list()
            .eval(Expr::Column(PlSmallStr::EMPTY).filter(predicate.inner.clone()))
            .into()
    }

    pub fn list_get(&self, index: &RbExpr, null_on_oob: bool) -> Self {
        self.inner
            .clone()
            .list()
            .get(index.inner.clone(), null_on_oob)
            .into()
    }

    pub fn list_join(&self, separator: &RbExpr, ignore_nulls: bool) -> Self {
        self.inner
            .clone()
            .list()
            .join(separator.inner.clone(), ignore_nulls)
            .into()
    }

    pub fn list_len(&self) -> Self {
        self.inner.clone().list().len().into()
    }

    pub fn list_max(&self) -> Self {
        self.inner.clone().list().max().into()
    }

    pub fn list_mean(&self) -> Self {
        self.inner.clone().list().mean().into()
    }

    pub fn list_median(&self) -> Self {
        self.inner.clone().list().median().into()
    }

    pub fn list_std(&self, ddof: u8) -> Self {
        self.inner.clone().list().std(ddof).into()
    }

    pub fn list_var(&self, ddof: u8) -> Self {
        self.inner.clone().list().var(ddof).into()
    }

    pub fn list_min(&self) -> Self {
        self.inner.clone().list().min().into()
    }

    pub fn list_reverse(&self) -> Self {
        self.inner.clone().list().reverse().into()
    }

    pub fn list_shift(&self, periods: &RbExpr) -> Self {
        self.inner
            .clone()
            .list()
            .shift(periods.inner.clone())
            .into()
    }

    pub fn list_slice(&self, offset: &RbExpr, length: Option<&RbExpr>) -> Self {
        let length = match length {
            Some(i) => i.inner.clone(),
            None => lit(i64::MAX),
        };
        self.inner
            .clone()
            .list()
            .slice(offset.inner.clone(), length)
            .into()
    }

    pub fn list_tail(&self, n: &RbExpr) -> Self {
        self.inner.clone().list().tail(n.inner.clone()).into()
    }

    pub fn list_sort(&self, descending: bool, nulls_last: bool) -> Self {
        self.inner
            .clone()
            .list()
            .sort(
                SortOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last),
            )
            .into()
    }

    pub fn list_sum(&self) -> Self {
        self.inner.clone().list().sum().into()
    }

    pub fn list_drop_nulls(&self) -> Self {
        self.inner.clone().list().drop_nulls().into()
    }

    pub fn list_sample_n(
        &self,
        n: &RbExpr,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .list()
            .sample_n(n.inner.clone(), with_replacement, shuffle, seed)
            .into()
    }

    pub fn list_sample_fraction(
        &self,
        fraction: &RbExpr,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .list()
            .sample_fraction(fraction.inner.clone(), with_replacement, shuffle, seed)
            .into()
    }

    pub fn list_gather(&self, index: &RbExpr, null_on_oob: bool) -> Self {
        self.inner
            .clone()
            .list()
            .gather(index.inner.clone(), null_on_oob)
            .into()
    }

    pub fn list_gather_every(&self, n: &RbExpr, offset: &RbExpr) -> Self {
        self.inner
            .clone()
            .list()
            .gather_every(n.inner.clone(), offset.inner.clone())
            .into()
    }

    pub fn list_to_array(&self, width: usize) -> Self {
        self.inner.clone().list().to_array(width).into()
    }

    pub fn list_to_struct(&self, names: RArray) -> RbResult<Self> {
        Ok(self
            .inner
            .clone()
            .list()
            .to_struct(
                names
                    .into_iter()
                    .map(|x| Ok(Wrap::<PlSmallStr>::try_convert(x)?.0))
                    .collect::<RbResult<Arc<[_]>>>()?,
            )
            .into())
    }

    pub fn list_n_unique(&self) -> Self {
        self.inner.clone().list().n_unique().into()
    }

    pub fn list_unique(&self, maintain_order: bool) -> Self {
        let e = self.inner.clone();

        if maintain_order {
            e.list().unique_stable().into()
        } else {
            e.list().unique().into()
        }
    }

    pub fn list_set_operation(&self, other: &RbExpr, operation: Wrap<SetOperation>) -> Self {
        let e = self.inner.clone().list();
        match operation.0 {
            SetOperation::Intersection => e.set_intersection(other.inner.clone()),
            SetOperation::Difference => e.set_difference(other.inner.clone()),
            SetOperation::Union => e.union(other.inner.clone()),
            SetOperation::SymmetricDifference => e.set_symmetric_difference(other.inner.clone()),
        }
        .into()
    }
}

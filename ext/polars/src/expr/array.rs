use polars::prelude::*;

use crate::{RbExpr, RbPolarsErr, RbResult};

impl RbExpr {
    pub fn arr_len(&self) -> Self {
        self.inner.clone().arr().len().into()
    }

    pub fn arr_max(&self) -> Self {
        self.inner.clone().arr().max().into()
    }

    pub fn arr_min(&self) -> Self {
        self.inner.clone().arr().min().into()
    }

    pub fn arr_sum(&self) -> Self {
        self.inner.clone().arr().sum().into()
    }

    pub fn arr_std(&self, ddof: u8) -> Self {
        self.inner.clone().arr().std(ddof).into()
    }

    pub fn arr_var(&self, ddof: u8) -> Self {
        self.inner.clone().arr().var(ddof).into()
    }

    pub fn arr_mean(&self) -> Self {
        self.inner.clone().arr().mean().into()
    }

    pub fn arr_median(&self) -> Self {
        self.inner.clone().arr().median().into()
    }

    pub fn arr_unique(&self, maintain_order: bool) -> Self {
        if maintain_order {
            self.inner.clone().arr().unique_stable().into()
        } else {
            self.inner.clone().arr().unique().into()
        }
    }

    pub fn arr_n_unique(&self) -> Self {
        self.inner.clone().arr().n_unique().into()
    }

    pub fn arr_to_list(&self) -> Self {
        self.inner.clone().arr().to_list().into()
    }

    pub fn arr_all(&self) -> Self {
        self.inner.clone().arr().all().into()
    }

    pub fn arr_any(&self) -> Self {
        self.inner.clone().arr().any().into()
    }

    pub fn arr_sort(&self, descending: bool, nulls_last: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .sort(SortOptions {
                descending,
                nulls_last,
                ..Default::default()
            })
            .into()
    }

    pub fn arr_reverse(&self) -> Self {
        self.inner.clone().arr().reverse().into()
    }

    pub fn arr_arg_min(&self) -> Self {
        self.inner.clone().arr().arg_min().into()
    }

    pub fn arr_arg_max(&self) -> Self {
        self.inner.clone().arr().arg_max().into()
    }

    pub fn arr_get(&self, index: &RbExpr, null_on_oob: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .get(index.inner.clone(), null_on_oob)
            .into()
    }

    pub fn arr_join(&self, separator: &RbExpr, ignore_nulls: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .join(separator.inner.clone(), ignore_nulls)
            .into()
    }

    pub fn arr_contains(&self, other: &RbExpr, nulls_equal: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .contains(other.inner.clone(), nulls_equal)
            .into()
    }

    pub fn arr_count_matches(&self, expr: &RbExpr) -> Self {
        self.inner
            .clone()
            .arr()
            .count_matches(expr.inner.clone())
            .into()
    }

    pub fn arr_slice(
        &self,
        offset: &RbExpr,
        length: Option<&RbExpr>,
        as_array: bool,
    ) -> RbResult<Self> {
        let length = match length {
            Some(i) => i.inner.clone(),
            None => lit(i64::MAX),
        };
        Ok(self
            .inner
            .clone()
            .arr()
            .slice(offset.inner.clone(), length, as_array)
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn arr_tail(&self, n: &RbExpr, as_array: bool) -> RbResult<Self> {
        Ok(self
            .inner
            .clone()
            .arr()
            .tail(n.inner.clone(), as_array)
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn arr_shift(&self, n: &RbExpr) -> Self {
        self.inner.clone().arr().shift(n.inner.clone()).into()
    }

    pub fn arr_explode(&self) -> Self {
        self.inner.clone().arr().explode().into()
    }
}

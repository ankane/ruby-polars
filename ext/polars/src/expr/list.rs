use magnus::Value;
use polars::lazy::dsl::lit;
use polars::prelude::*;
use polars::series::ops::NullBehavior;

use crate::conversion::Wrap;
use crate::{RbExpr, RbResult};

impl RbExpr {
    pub fn list_arg_max(&self) -> Self {
        self.inner.clone().arr().arg_max().into()
    }

    pub fn list_arg_min(&self) -> Self {
        self.inner.clone().arr().arg_min().into()
    }

    pub fn list_contains(&self, other: &RbExpr) -> Self {
        self.inner
            .clone()
            .arr()
            .contains(other.inner.clone())
            .into()
    }

    pub fn list_count_match(&self, expr: &RbExpr) -> Self {
        self.inner.clone().arr().count_match(expr.inner.clone()).into()
    }

    pub fn list_diff(&self, n: i64, null_behavior: Wrap<NullBehavior>) -> RbResult<Self> {
        Ok(self.inner.clone().arr().diff(n, null_behavior.0).into())
    }

    pub fn list_eval(&self, expr: &RbExpr, parallel: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .eval(expr.inner.clone(), parallel)
            .into()
    }

    pub fn list_get(&self, index: &RbExpr) -> Self {
        self.inner.clone().arr().get(index.inner.clone()).into()
    }

    pub fn list_join(&self, separator: String) -> Self {
        self.inner.clone().arr().join(&separator).into()
    }

    pub fn list_lengths(&self) -> Self {
        self.inner.clone().arr().lengths().into()
    }

    pub fn list_max(&self) -> Self {
        self.inner.clone().arr().max().into()
    }

    pub fn list_mean(&self) -> Self {
        self.inner.clone().arr().mean().with_fmt("arr.mean").into()
    }

    pub fn list_min(&self) -> Self {
        self.inner.clone().arr().min().into()
    }

    pub fn list_reverse(&self) -> Self {
        self.inner.clone().arr().reverse().into()
    }

    pub fn list_shift(&self, periods: i64) -> Self {
        self.inner.clone().arr().shift(periods).into()
    }

    pub fn list_slice(&self, offset: &RbExpr, length: Option<&RbExpr>) -> Self {
        let length = match length {
            Some(i) => i.inner.clone(),
            None => lit(i64::MAX),
        };
        self.inner
            .clone()
            .arr()
            .slice(offset.inner.clone(), length)
            .into()
    }

    pub fn list_sort(&self, reverse: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .sort(SortOptions {
                descending: reverse,
                ..Default::default()
            })
            .with_fmt("arr.sort")
            .into()
    }

    pub fn list_sum(&self) -> Self {
        self.inner.clone().arr().sum().with_fmt("arr.sum").into()
    }

    pub fn list_take(&self, index: &RbExpr, null_on_oob: bool) -> Self {
        self.inner
            .clone()
            .arr()
            .take(index.inner.clone(), null_on_oob)
            .into()
    }

    pub fn list_to_struct(
        &self,
        width_strat: Wrap<ListToStructWidthStrategy>,
        _name_gen: Option<Value>,
        upper_bound: usize,
    ) -> RbResult<Self> {
        // TODO fix
        let name_gen = None;
        // let name_gen = name_gen.map(|lambda| {
        //     Arc::new(move |idx: usize| {
        //         let out: Value = lambda.funcall("call", (idx,)).unwrap();
        //         out.try_convert::<String>().unwrap()
        //     }) as NameGenerator
        // });

        Ok(self
            .inner
            .clone()
            .arr()
            .to_struct(width_strat.0, name_gen, upper_bound)
            .into())
    }

    pub fn list_unique(&self, maintain_order: bool) -> Self {
        let e = self.inner.clone();

        if maintain_order {
            e.arr().unique_stable().into()
        } else {
            e.arr().unique().into()
        }
    }
}

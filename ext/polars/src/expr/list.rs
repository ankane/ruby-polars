use magnus::{prelude::*, value::Opaque, Ruby, Value};
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

    pub fn list_to_array(&self, width: usize) -> Self {
        self.inner.clone().list().to_array(width).into()
    }

    pub fn list_to_struct(
        &self,
        width_strat: Wrap<ListToStructWidthStrategy>,
        name_gen: Option<Value>,
        upper_bound: Option<usize>,
    ) -> RbResult<Self> {
        let name_gen = name_gen.map(|lambda| {
            let lambda = Opaque::from(lambda);
            Arc::new(move |idx: usize| {
                let lambda = Ruby::get().unwrap().get_inner(lambda);
                let out: String = lambda.funcall("call", (idx,)).unwrap();
                PlSmallStr::from_string(out)
            });

            // non-Ruby thread
            todo!();
        });

        Ok(self
            .inner
            .clone()
            .list()
            .to_struct(ListToStructArgs::InferWidth {
                infer_field_strategy: width_strat.0,
                get_index_name: name_gen,
                max_fields: upper_bound,
            })
            .into())
    }

    pub fn list_unique(&self, maintain_order: bool) -> Self {
        let e = self.inner.clone();

        if maintain_order {
            e.list().unique_stable().into()
        } else {
            e.list().unique().into()
        }
    }
}

use magnus::{RArray, Value};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::conversion::{parse_fill_null_strategy, Wrap};
use crate::rb_exprs_to_exprs;
use crate::{RbExpr, RbResult};

impl RbExpr {
    pub fn add(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Plus, rhs.inner.clone()).into())
    }

    pub fn sub(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Minus, rhs.inner.clone()).into())
    }

    pub fn mul(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into())
    }

    pub fn truediv(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::TrueDivide, rhs.inner.clone()).into())
    }

    pub fn _mod(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Modulus, rhs.inner.clone()).into())
    }

    pub fn floordiv(&self, rhs: &RbExpr) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::FloorDivide, rhs.inner.clone()).into())
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

    pub fn is_infinite(&self) -> Self {
        self.clone().inner.is_infinite().into()
    }

    pub fn is_finite(&self) -> Self {
        self.clone().inner.is_finite().into()
    }

    pub fn is_nan(&self) -> Self {
        self.clone().inner.is_nan().into()
    }

    pub fn is_not_nan(&self) -> Self {
        self.clone().inner.is_not_nan().into()
    }

    pub fn min(&self) -> Self {
        self.clone().inner.min().into()
    }

    pub fn max(&self) -> Self {
        self.clone().inner.max().into()
    }

    pub fn nan_max(&self) -> Self {
        self.clone().inner.nan_max().into()
    }

    pub fn nan_min(&self) -> Self {
        self.clone().inner.nan_min().into()
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

    pub fn arg_unique(&self) -> Self {
        self.clone().inner.arg_unique().into()
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

    pub fn implode(&self) -> Self {
        self.clone().inner.implode().into()
    }

    pub fn quantile(
        &self,
        quantile: &RbExpr,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> Self {
        self.clone()
            .inner
            .quantile(quantile.inner.clone(), interpolation.0)
            .into()
    }

    pub fn agg_groups(&self) -> Self {
        self.clone().inner.agg_groups().into()
    }

    pub fn count(&self) -> Self {
        self.clone().inner.count().into()
    }

    pub fn value_counts(&self, multithreaded: bool, sorted: bool) -> Self {
        self.inner
            .clone()
            .value_counts(multithreaded, sorted)
            .into()
    }

    pub fn unique_counts(&self) -> Self {
        self.inner.clone().unique_counts().into()
    }

    pub fn null_count(&self) -> Self {
        self.inner.clone().null_count().into()
    }

    pub fn cast(&self, data_type: Wrap<DataType>, strict: bool) -> RbResult<Self> {
        let dt = data_type.0;
        let expr = if strict {
            self.inner.clone().strict_cast(dt)
        } else {
            self.inner.clone().cast(dt)
        };
        Ok(expr.into())
    }

    pub fn sort_with(&self, descending: bool, nulls_last: bool) -> Self {
        self.clone()
            .inner
            .sort_with(SortOptions {
                descending,
                nulls_last,
                multithreaded: true,
            })
            .into()
    }

    pub fn arg_sort(&self, reverse: bool, nulls_last: bool) -> Self {
        self.clone()
            .inner
            .arg_sort(SortOptions {
                descending: reverse,
                nulls_last,
                multithreaded: true,
            })
            .into()
    }

    pub fn top_k(&self, k: usize) -> Self {
        self.inner.clone().top_k(k).into()
    }

    pub fn bottom_k(&self, k: usize) -> Self {
        self.inner.clone().bottom_k(k).into()
    }

    pub fn arg_max(&self) -> Self {
        self.clone().inner.arg_max().into()
    }

    pub fn arg_min(&self) -> Self {
        self.clone().inner.arg_min().into()
    }

    pub fn search_sorted(&self, element: &RbExpr, side: Wrap<SearchSortedSide>) -> Self {
        self.inner
            .clone()
            .search_sorted(element.inner.clone(), side.0)
            .into()
    }

    pub fn take(&self, idx: &RbExpr) -> Self {
        self.clone().inner.take(idx.inner.clone()).into()
    }

    pub fn sort_by(&self, by: RArray, reverse: Vec<bool>) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self.clone().inner.sort_by(by, reverse).into())
    }

    pub fn backward_fill(&self, limit: FillNullLimit) -> Self {
        self.clone().inner.backward_fill(limit).into()
    }

    pub fn forward_fill(&self, limit: FillNullLimit) -> Self {
        self.clone().inner.forward_fill(limit).into()
    }

    pub fn shift(&self, periods: i64) -> Self {
        self.clone().inner.shift(periods).into()
    }
    pub fn shift_and_fill(&self, periods: i64, fill_value: &RbExpr) -> Self {
        self.clone()
            .inner
            .shift_and_fill(periods, fill_value.inner.clone())
            .into()
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
            .apply(
                move |s| s.fill_null(strat).map(Some),
                GetOutput::same_type(),
            )
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

    pub fn is_unique(&self) -> Self {
        self.clone().inner.is_unique().into()
    }

    pub fn approx_unique(&self) -> Self {
        self.clone().inner.approx_unique().into()
    }

    pub fn is_first(&self) -> Self {
        self.clone().inner.is_first().into()
    }

    pub fn explode(&self) -> Self {
        self.clone().inner.explode().into()
    }

    pub fn take_every(&self, n: usize) -> Self {
        self.clone()
            .inner
            .map(
                move |s: Series| Ok(Some(s.take_every(n))),
                GetOutput::same_type(),
            )
            .with_fmt("take_every")
            .into()
    }

    pub fn tail(&self, n: Option<usize>) -> Self {
        self.clone().inner.tail(n).into()
    }

    pub fn head(&self, n: Option<usize>) -> Self {
        self.clone().inner.head(n).into()
    }

    pub fn slice(&self, offset: &RbExpr, length: &RbExpr) -> Self {
        self.inner
            .clone()
            .slice(offset.inner.clone(), length.inner.clone())
            .into()
    }

    pub fn append(&self, other: &RbExpr, upcast: bool) -> Self {
        self.inner
            .clone()
            .append(other.inner.clone(), upcast)
            .into()
    }

    pub fn rechunk(&self) -> Self {
        self.inner
            .clone()
            .map(|s| Ok(Some(s.rechunk())), GetOutput::same_type())
            .into()
    }

    pub fn round(&self, decimals: u32) -> Self {
        self.clone().inner.round(decimals).into()
    }

    pub fn floor(&self) -> Self {
        self.clone().inner.floor().into()
    }

    pub fn ceil(&self) -> Self {
        self.clone().inner.ceil().into()
    }

    pub fn clip(&self, min: Value, max: Value) -> Self {
        let min = min.try_convert::<Wrap<AnyValue>>().unwrap().0;
        let max = max.try_convert::<Wrap<AnyValue>>().unwrap().0;
        self.clone().inner.clip(min, max).into()
    }

    pub fn clip_min(&self, min: Value) -> Self {
        let min = min.try_convert::<Wrap<AnyValue>>().unwrap().0;
        self.clone().inner.clip_min(min).into()
    }

    pub fn clip_max(&self, max: Value) -> Self {
        let max = max.try_convert::<Wrap<AnyValue>>().unwrap().0;
        self.clone().inner.clip_max(max).into()
    }

    pub fn abs(&self) -> Self {
        self.clone().inner.abs().into()
    }

    pub fn sin(&self) -> Self {
        self.clone().inner.sin().into()
    }

    pub fn cos(&self) -> Self {
        self.clone().inner.cos().into()
    }

    pub fn tan(&self) -> Self {
        self.clone().inner.tan().into()
    }

    pub fn arcsin(&self) -> Self {
        self.clone().inner.arcsin().into()
    }

    pub fn arccos(&self) -> Self {
        self.clone().inner.arccos().into()
    }

    pub fn arctan(&self) -> Self {
        self.clone().inner.arctan().into()
    }

    pub fn sinh(&self) -> Self {
        self.clone().inner.sinh().into()
    }

    pub fn cosh(&self) -> Self {
        self.clone().inner.cosh().into()
    }

    pub fn tanh(&self) -> Self {
        self.clone().inner.tanh().into()
    }

    pub fn arcsinh(&self) -> Self {
        self.clone().inner.arcsinh().into()
    }

    pub fn arccosh(&self) -> Self {
        self.clone().inner.arccosh().into()
    }

    pub fn arctanh(&self) -> Self {
        self.clone().inner.arctanh().into()
    }

    pub fn sign(&self) -> Self {
        self.clone().inner.sign().into()
    }

    pub fn is_duplicated(&self) -> Self {
        self.clone().inner.is_duplicated().into()
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

    pub fn is_in(&self, expr: &RbExpr) -> Self {
        self.clone().inner.is_in(expr.inner.clone()).into()
    }

    pub fn repeat_by(&self, by: &RbExpr) -> Self {
        self.clone().inner.repeat_by(by.inner.clone()).into()
    }

    pub fn pow(&self, exponent: &RbExpr) -> Self {
        self.clone().inner.pow(exponent.inner.clone()).into()
    }

    pub fn cumsum(&self, reverse: bool) -> Self {
        self.clone().inner.cumsum(reverse).into()
    }

    pub fn cummax(&self, reverse: bool) -> Self {
        self.clone().inner.cummax(reverse).into()
    }

    pub fn cummin(&self, reverse: bool) -> Self {
        self.clone().inner.cummin(reverse).into()
    }

    pub fn cumprod(&self, reverse: bool) -> Self {
        self.clone().inner.cumprod(reverse).into()
    }

    pub fn product(&self) -> Self {
        self.clone().inner.product().into()
    }

    pub fn shrink_dtype(&self) -> Self {
        self.inner.clone().shrink_dtype().into()
    }
}

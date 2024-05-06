use std::ops::Neg;

use magnus::{prelude::*, value::Opaque, IntoValue, RArray, Ruby, Value};
use polars::lazy::dsl;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_core::series::IsSorted;

use crate::conversion::{parse_fill_null_strategy, Wrap};
use crate::map::lazy::map_single;
use crate::rb_exprs_to_exprs;
use crate::utils::reinterpret;
use crate::{RbExpr, RbResult};

impl RbExpr {
    pub fn add(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Plus, rhs.inner.clone()).into())
    }

    pub fn sub(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Minus, rhs.inner.clone()).into())
    }

    pub fn mul(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into())
    }

    pub fn truediv(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::TrueDivide, rhs.inner.clone()).into())
    }

    pub fn _mod(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Modulus, rhs.inner.clone()).into())
    }

    pub fn floordiv(&self, rhs: &Self) -> RbResult<Self> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::FloorDivide, rhs.inner.clone()).into())
    }

    pub fn neg(&self) -> RbResult<Self> {
        Ok(self.inner.clone().neg().into())
    }

    pub fn to_str(&self) -> String {
        format!("{:?}", self.inner)
    }

    pub fn eq(&self, other: &Self) -> Self {
        self.clone().inner.eq(other.inner.clone()).into()
    }

    pub fn eq_missing(&self, other: &Self) -> Self {
        self.inner.clone().eq_missing(other.inner.clone()).into()
    }

    pub fn neq(&self, other: &Self) -> Self {
        self.clone().inner.neq(other.inner.clone()).into()
    }

    pub fn neq_missing(&self, other: &Self) -> Self {
        self.inner.clone().neq_missing(other.inner.clone()).into()
    }

    pub fn gt(&self, other: &Self) -> Self {
        self.clone().inner.gt(other.inner.clone()).into()
    }

    pub fn gt_eq(&self, other: &Self) -> Self {
        self.clone().inner.gt_eq(other.inner.clone()).into()
    }

    pub fn lt_eq(&self, other: &Self) -> Self {
        self.clone().inner.lt_eq(other.inner.clone()).into()
    }

    pub fn lt(&self, other: &Self) -> Self {
        self.clone().inner.lt(other.inner.clone()).into()
    }

    pub fn alias(&self, name: String) -> Self {
        self.clone().inner.alias(&name).into()
    }

    pub fn not_(&self) -> Self {
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

    pub fn quantile(&self, quantile: &Self, interpolation: Wrap<QuantileInterpolOptions>) -> Self {
        self.clone()
            .inner
            .quantile(quantile.inner.clone(), interpolation.0)
            .into()
    }

    pub fn cut(
        &self,
        breaks: Vec<f64>,
        labels: Option<Vec<String>>,
        left_closed: bool,
        include_breaks: bool,
    ) -> Self {
        self.inner
            .clone()
            .cut(breaks, labels, left_closed, include_breaks)
            .into()
    }

    pub fn qcut(
        &self,
        probs: Vec<f64>,
        labels: Option<Vec<String>>,
        left_closed: bool,
        allow_duplicates: bool,
        include_breaks: bool,
    ) -> Self {
        self.inner
            .clone()
            .qcut(probs, labels, left_closed, allow_duplicates, include_breaks)
            .into()
    }

    pub fn qcut_uniform(
        &self,
        n_bins: usize,
        labels: Option<Vec<String>>,
        left_closed: bool,
        allow_duplicates: bool,
        include_breaks: bool,
    ) -> Self {
        self.inner
            .clone()
            .qcut_uniform(
                n_bins,
                labels,
                left_closed,
                allow_duplicates,
                include_breaks,
            )
            .into()
    }

    pub fn rle(&self) -> Self {
        self.inner.clone().rle().into()
    }

    pub fn rle_id(&self) -> Self {
        self.inner.clone().rle_id().into()
    }

    pub fn agg_groups(&self) -> Self {
        self.inner.clone().agg_groups().into()
    }

    pub fn count(&self) -> Self {
        self.inner.clone().count().into()
    }

    pub fn len(&self) -> Self {
        self.inner.clone().len().into()
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
            .sort(SortOptions {
                descending,
                nulls_last,
                multithreaded: true,
                maintain_order: false,
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
                maintain_order: false,
            })
            .into()
    }

    pub fn top_k(&self, k: &Self) -> Self {
        self.inner.clone().top_k(k.inner.clone()).into()
    }

    pub fn bottom_k(&self, k: &Self) -> Self {
        self.inner.clone().bottom_k(k.inner.clone()).into()
    }

    pub fn peak_min(&self) -> Self {
        self.inner.clone().peak_min().into()
    }

    pub fn peak_max(&self) -> Self {
        self.inner.clone().peak_max().into()
    }

    pub fn arg_max(&self) -> Self {
        self.clone().inner.arg_max().into()
    }

    pub fn arg_min(&self) -> Self {
        self.clone().inner.arg_min().into()
    }

    pub fn search_sorted(&self, element: &Self, side: Wrap<SearchSortedSide>) -> Self {
        self.inner
            .clone()
            .search_sorted(element.inner.clone(), side.0)
            .into()
    }

    pub fn gather(&self, idx: &Self) -> Self {
        self.clone().inner.gather(idx.inner.clone()).into()
    }

    pub fn sort_by(
        &self,
        by: RArray,
        descending: Vec<bool>,
        nulls_last: bool,
        multithreaded: bool,
        maintain_order: bool,
    ) -> RbResult<Self> {
        let by = rb_exprs_to_exprs(by)?;
        Ok(self
            .clone()
            .inner
            .sort_by(
                by,
                SortMultipleOptions {
                    descending,
                    nulls_last,
                    multithreaded,
                    maintain_order,
                },
            )
            .into())
    }

    pub fn backward_fill(&self, limit: FillNullLimit) -> Self {
        self.clone().inner.backward_fill(limit).into()
    }

    pub fn forward_fill(&self, limit: FillNullLimit) -> Self {
        self.clone().inner.forward_fill(limit).into()
    }

    pub fn shift(&self, n: &Self, fill_value: Option<&Self>) -> Self {
        let expr = self.inner.clone();
        let out = match fill_value {
            Some(v) => expr.shift_and_fill(n.inner.clone(), v.inner.clone()),
            None => expr.shift(n.inner.clone()),
        };
        out.into()
    }

    pub fn fill_null(&self, expr: &Self) -> Self {
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

    pub fn fill_nan(&self, expr: &Self) -> Self {
        self.inner.clone().fill_nan(expr.inner.clone()).into()
    }

    pub fn drop_nulls(&self) -> Self {
        self.inner.clone().drop_nulls().into()
    }

    pub fn drop_nans(&self) -> Self {
        self.inner.clone().drop_nans().into()
    }

    pub fn filter(&self, predicate: &Self) -> Self {
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

    pub fn approx_n_unique(&self) -> Self {
        self.clone().inner.approx_n_unique().into()
    }

    pub fn is_first_distinct(&self) -> Self {
        self.clone().inner.is_first_distinct().into()
    }

    pub fn is_last_distinct(&self) -> Self {
        self.clone().inner.is_last_distinct().into()
    }

    pub fn explode(&self) -> Self {
        self.clone().inner.explode().into()
    }

    pub fn gather_every(&self, n: usize, offset: usize) -> Self {
        self.clone()
            .inner
            .map(
                move |s: Series| Ok(Some(s.gather_every(n, offset))),
                GetOutput::same_type(),
            )
            .with_fmt("gather_every")
            .into()
    }

    pub fn tail(&self, n: Option<usize>) -> Self {
        self.clone().inner.tail(n).into()
    }

    pub fn head(&self, n: Option<usize>) -> Self {
        self.clone().inner.head(n).into()
    }

    pub fn slice(&self, offset: &Self, length: &Self) -> Self {
        self.inner
            .clone()
            .slice(offset.inner.clone(), length.inner.clone())
            .into()
    }

    pub fn append(&self, other: &Self, upcast: bool) -> Self {
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

    pub fn clip(&self, min: Option<&Self>, max: Option<&Self>) -> Self {
        let expr = self.inner.clone();
        let out = match (min, max) {
            (Some(min), Some(max)) => expr.clip(min.inner.clone(), max.inner.clone()),
            (Some(min), None) => expr.clip_min(min.inner.clone()),
            (None, Some(max)) => expr.clip_max(max.inner.clone()),
            (None, None) => expr,
        };
        out.into()
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

    pub fn _and(&self, expr: &Self) -> Self {
        self.clone().inner.and(expr.inner.clone()).into()
    }

    pub fn _xor(&self, expr: &Self) -> Self {
        self.clone().inner.xor(expr.inner.clone()).into()
    }

    pub fn _or(&self, expr: &Self) -> Self {
        self.clone().inner.or(expr.inner.clone()).into()
    }

    pub fn is_in(&self, expr: &Self) -> Self {
        self.clone().inner.is_in(expr.inner.clone()).into()
    }

    pub fn repeat_by(&self, by: &Self) -> Self {
        self.clone().inner.repeat_by(by.inner.clone()).into()
    }

    pub fn pow(&self, exponent: &Self) -> Self {
        self.clone().inner.pow(exponent.inner.clone()).into()
    }

    pub fn cum_sum(&self, reverse: bool) -> Self {
        self.clone().inner.cum_sum(reverse).into()
    }

    pub fn cum_max(&self, reverse: bool) -> Self {
        self.clone().inner.cum_max(reverse).into()
    }

    pub fn cum_min(&self, reverse: bool) -> Self {
        self.clone().inner.cum_min(reverse).into()
    }

    pub fn cum_prod(&self, reverse: bool) -> Self {
        self.clone().inner.cum_prod(reverse).into()
    }

    pub fn product(&self) -> Self {
        self.clone().inner.product().into()
    }

    pub fn shrink_dtype(&self) -> Self {
        self.inner.clone().shrink_dtype().into()
    }

    pub fn map_batches(
        &self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        agg_list: bool,
        is_elementwise: bool,
    ) -> Self {
        map_single(self, lambda, output_type, agg_list, is_elementwise)
    }

    pub fn dot(&self, other: &Self) -> Self {
        self.inner.clone().dot(other.inner.clone()).into()
    }

    pub fn reinterpret(&self, signed: bool) -> Self {
        let function = move |s: Series| reinterpret(&s, signed).map(Some);
        let dt = if signed {
            DataType::Int64
        } else {
            DataType::UInt64
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(dt))
            .into()
    }

    pub fn mode(&self) -> Self {
        self.inner.clone().mode().into()
    }

    pub fn exclude(&self, columns: Vec<String>) -> Self {
        self.inner.clone().exclude(columns).into()
    }

    pub fn interpolate(&self, method: Wrap<InterpolationMethod>) -> Self {
        self.inner.clone().interpolate(method.0).into()
    }

    pub fn lower_bound(&self) -> Self {
        self.inner.clone().lower_bound().into()
    }

    pub fn upper_bound(&self) -> Self {
        self.inner.clone().upper_bound().into()
    }

    pub fn cumulative_eval(&self, expr: &Self, min_periods: usize, parallel: bool) -> Self {
        self.inner
            .clone()
            .cumulative_eval(expr.inner.clone(), min_periods, parallel)
            .into()
    }

    pub fn rank(&self, method: Wrap<RankMethod>, reverse: bool, seed: Option<u64>) -> Self {
        let options = RankOptions {
            method: method.0,
            descending: reverse,
        };
        self.inner.clone().rank(options, seed).into()
    }

    pub fn diff(&self, n: i64, null_behavior: Wrap<NullBehavior>) -> Self {
        self.inner.clone().diff(n, null_behavior.0).into()
    }

    pub fn pct_change(&self, n: &Self) -> Self {
        self.inner.clone().pct_change(n.inner.clone()).into()
    }

    pub fn skew(&self, bias: bool) -> Self {
        self.inner.clone().skew(bias).into()
    }

    pub fn kurtosis(&self, fisher: bool, bias: bool) -> Self {
        self.inner.clone().kurtosis(fisher, bias).into()
    }

    pub fn reshape(&self, dims: Vec<i64>) -> Self {
        self.inner.clone().reshape(&dims).into()
    }

    pub fn cum_count(&self, reverse: bool) -> Self {
        self.inner.clone().cum_count(reverse).into()
    }

    pub fn to_physical(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.to_physical_repr().into_owned())),
                GetOutput::map_dtype(|dt| dt.to_physical()),
            )
            .with_fmt("to_physical")
            .into()
    }

    pub fn shuffle(&self, seed: Option<u64>) -> Self {
        self.inner.clone().shuffle(seed).into()
    }

    pub fn sample_n(
        &self,
        n: &Self,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_n(n.inner.clone(), with_replacement, shuffle, seed)
            .into()
    }

    pub fn sample_frac(
        &self,
        frac: &Self,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_frac(frac.inner.clone(), with_replacement, shuffle, seed)
            .into()
    }

    pub fn ewm_mean(
        &self,
        alpha: f64,
        adjust: bool,
        min_periods: usize,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias: false,
            min_periods,
            ignore_nulls,
        };
        self.inner.clone().ewm_mean(options).into()
    }

    pub fn ewm_std(
        &self,
        alpha: f64,
        adjust: bool,
        bias: bool,
        min_periods: usize,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods,
            ignore_nulls,
        };
        self.inner.clone().ewm_std(options).into()
    }

    pub fn ewm_var(
        &self,
        alpha: f64,
        adjust: bool,
        bias: bool,
        min_periods: usize,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods,
            ignore_nulls,
        };
        self.inner.clone().ewm_var(options).into()
    }

    pub fn extend_constant(&self, value: Wrap<AnyValue>, n: usize) -> Self {
        let value = value.into_value();
        let value = Opaque::from(value);
        self.inner
            .clone()
            .apply(
                move |s| {
                    let value = Ruby::get().unwrap().get_inner(value);
                    let value = Wrap::<AnyValue>::try_convert(value).unwrap().0;
                    s.extend_constant(value, n).map(Some)
                },
                GetOutput::same_type(),
            )
            .with_fmt("extend")
            .into()
    }

    pub fn any(&self, drop_nulls: bool) -> Self {
        self.inner.clone().any(drop_nulls).into()
    }

    pub fn all(&self, drop_nulls: bool) -> Self {
        self.inner.clone().all(drop_nulls).into()
    }

    pub fn log(&self, base: f64) -> Self {
        self.inner.clone().log(base).into()
    }

    pub fn exp(&self) -> Self {
        self.inner.clone().exp().into()
    }

    pub fn entropy(&self, base: f64, normalize: bool) -> Self {
        self.inner.clone().entropy(base, normalize).into()
    }

    pub fn hash(&self, seed: u64, seed_1: u64, seed_2: u64, seed_3: u64) -> Self {
        self.inner.clone().hash(seed, seed_1, seed_2, seed_3).into()
    }

    pub fn set_sorted_flag(&self, descending: bool) -> Self {
        let is_sorted = if descending {
            IsSorted::Descending
        } else {
            IsSorted::Ascending
        };
        self.inner.clone().set_sorted_flag(is_sorted).into()
    }

    pub fn replace(
        &self,
        old: &Self,
        new: &Self,
        default: Option<&Self>,
        return_dtype: Option<Wrap<DataType>>,
    ) -> Self {
        self.inner
            .clone()
            .replace(
                old.inner.clone(),
                new.inner.clone(),
                default.map(|e| e.inner.clone()),
                return_dtype.map(|dt| dt.0),
            )
            .into()
    }
}

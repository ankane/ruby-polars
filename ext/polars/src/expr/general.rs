use magnus::{block::Proc, prelude::*, value::Opaque, IntoValue, RArray, Ruby, Value};
use polars::lazy::dsl;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_core::series::IsSorted;
use std::any::Any;

use crate::apply::lazy::map_single;
use crate::conversion::{parse_fill_null_strategy, Wrap};
use crate::rb_exprs_to_exprs;
use crate::utils::reinterpret;
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
        let min = Wrap::<AnyValue>::try_convert(min).unwrap().0;
        let max = Wrap::<AnyValue>::try_convert(max).unwrap().0;
        self.clone().inner.clip(min, max).into()
    }

    pub fn clip_min(&self, min: Value) -> Self {
        let min = Wrap::<AnyValue>::try_convert(min).unwrap().0;
        self.clone().inner.clip_min(min).into()
    }

    pub fn clip_max(&self, max: Value) -> Self {
        let max = Wrap::<AnyValue>::try_convert(max).unwrap().0;
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

    pub fn map(&self, lambda: Value, output_type: Option<Wrap<DataType>>, agg_list: bool) -> Self {
        map_single(self, lambda, output_type, agg_list)
    }

    pub fn dot(&self, other: &RbExpr) -> Self {
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

    pub fn keep_name(&self) -> Self {
        self.inner.clone().keep_name().into()
    }

    pub fn prefix(&self, prefix: String) -> Self {
        self.inner.clone().prefix(&prefix).into()
    }

    pub fn suffix(&self, suffix: String) -> Self {
        self.inner.clone().suffix(&suffix).into()
    }

    pub fn map_alias(&self, lambda: Proc) -> Self {
        let lambda = Opaque::from(lambda);
        self.inner
            .clone()
            .map_alias(move |name| {
                let lambda = Ruby::get().unwrap().get_inner(lambda);
                let out = lambda.call::<_, String>((name,));
                match out {
                    Ok(out) => Ok(out),
                    Err(e) => Err(PolarsError::ComputeError(
                        format!("Ruby function in 'map_alias' produced an error: {}.", e).into(),
                    )),
                }
            })
            .into()
    }

    pub fn exclude(&self, columns: Vec<String>) -> Self {
        self.inner.clone().exclude(columns).into()
    }

    pub fn interpolate(&self, method: Wrap<InterpolationMethod>) -> Self {
        self.inner.clone().interpolate(method.0).into()
    }

    pub fn rolling_sum(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };
        self.inner.clone().rolling_sum(options).into()
    }

    pub fn rolling_min(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };
        self.inner.clone().rolling_min(options).into()
    }

    pub fn rolling_max(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };
        self.inner.clone().rolling_max(options).into()
    }

    pub fn rolling_mean(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            ..Default::default()
        };

        self.inner.clone().rolling_mean(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_std(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
        ddof: u8,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_std(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_var(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
        ddof: u8,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingVarParams { ddof }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_var(options).into()
    }

    pub fn rolling_median(
        &self,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingQuantileParams {
                prob: 0.5,
                interpol: QuantileInterpolOptions::Linear,
            }) as Arc<dyn Any + Send + Sync>),
        };
        self.inner.clone().rolling_quantile(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn rolling_quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: usize,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> Self {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            weights,
            min_periods,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingQuantileParams {
                prob: quantile,
                interpol: interpolation.0,
            }) as Arc<dyn Any + Send + Sync>),
        };

        self.inner.clone().rolling_quantile(options).into()
    }

    pub fn rolling_skew(&self, window_size: usize, bias: bool) -> Self {
        self.inner.clone().rolling_skew(window_size, bias).into()
    }

    pub fn lower_bound(&self) -> Self {
        self.inner.clone().lower_bound().into()
    }

    pub fn upper_bound(&self) -> Self {
        self.inner.clone().upper_bound().into()
    }

    pub fn cumulative_eval(&self, expr: &RbExpr, min_periods: usize, parallel: bool) -> Self {
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

    pub fn pct_change(&self, n: i64) -> Self {
        self.inner.clone().pct_change(n).into()
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

    pub fn cumcount(&self, reverse: bool) -> Self {
        self.inner.clone().cumcount(reverse).into()
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
        n: usize,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_n(n, with_replacement, shuffle, seed)
            .into()
    }

    pub fn sample_frac(
        &self,
        frac: f64,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> Self {
        self.inner
            .clone()
            .sample_frac(frac, with_replacement, shuffle, seed)
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
}

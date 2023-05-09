use magnus::block::Proc;
use magnus::{class, IntoValue, RArray, RString, Value};
use polars::lazy::dsl;
use polars::prelude::*;
use polars::series::ops::NullBehavior;

use crate::conversion::*;
use crate::lazy::apply::*;
use crate::lazy::utils::rb_exprs_to_exprs;
use crate::utils::reinterpret;
use crate::{RbExpr, RbPolarsErr, RbResult, RbSeries};

impl RbExpr {
    pub fn strftime(&self, fmt: String) -> Self {
        self.inner.clone().dt().strftime(&fmt).into()
    }

    pub fn year(&self) -> Self {
        self.clone().inner.dt().year().into()
    }

    pub fn iso_year(&self) -> Self {
        self.clone().inner.dt().iso_year().into()
    }

    pub fn quarter(&self) -> Self {
        self.clone().inner.dt().quarter().into()
    }

    pub fn month(&self) -> Self {
        self.clone().inner.dt().month().into()
    }

    pub fn week(&self) -> Self {
        self.clone().inner.dt().week().into()
    }

    pub fn weekday(&self) -> Self {
        self.clone().inner.dt().weekday().into()
    }

    pub fn day(&self) -> Self {
        self.clone().inner.dt().day().into()
    }

    pub fn ordinal_day(&self) -> Self {
        self.clone().inner.dt().ordinal_day().into()
    }

    pub fn hour(&self) -> Self {
        self.clone().inner.dt().hour().into()
    }

    pub fn minute(&self) -> Self {
        self.clone().inner.dt().minute().into()
    }

    pub fn second(&self) -> Self {
        self.clone().inner.dt().second().into()
    }

    pub fn millisecond(&self) -> Self {
        self.clone().inner.dt().millisecond().into()
    }

    pub fn microsecond(&self) -> Self {
        self.clone().inner.dt().microsecond().into()
    }

    pub fn nanosecond(&self) -> Self {
        self.clone().inner.dt().nanosecond().into()
    }

    pub fn duration_days(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.days().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_hours(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.hours().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_minutes(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.minutes().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_seconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.seconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_nanoseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.nanoseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_microseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.microseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_milliseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.milliseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn timestamp(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().timestamp(tu.0).into()
    }

    pub fn dt_offset_by(&self, by: String) -> Self {
        let by = Duration::parse(&by);
        self.inner.clone().dt().offset_by(by).into()
    }

    pub fn dt_epoch_seconds(&self) -> Self {
        self.clone()
            .inner
            .map(
                |s| {
                    s.timestamp(TimeUnit::Milliseconds)
                        .map(|ca| Some((ca / 1000).into_series()))
                },
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn dt_with_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().with_time_unit(tu.0).into()
    }

    pub fn dt_convert_time_zone(&self, tz: TimeZone) -> Self {
        self.inner.clone().dt().convert_time_zone(tz).into()
    }

    pub fn dt_cast_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().cast_time_unit(tu.0).into()
    }

    pub fn dt_replace_time_zone(&self, tz: Option<String>, use_earliest: Option<bool>) -> Self {
        self.inner.clone().dt().replace_time_zone(tz, use_earliest).into()
    }

    #[allow(deprecated)]
    pub fn dt_tz_localize(&self, tz: String) -> Self {
        self.inner.clone().dt().tz_localize(tz).into()
    }

    pub fn dt_truncate(&self, every: String, offset: String) -> Self {
        self.inner.clone().dt().truncate(&every, &offset).into()
    }

    pub fn dt_round(&self, every: String, offset: String) -> Self {
        self.inner.clone().dt().round(&every, &offset).into()
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
        self.inner
            .clone()
            .map_alias(move |name| {
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
        };

        self.inner.clone().rolling_mean(options).into()
    }

    pub fn rolling_std(
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
        };

        self.inner.clone().rolling_std(options).into()
    }

    pub fn rolling_var(
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
        };
        self.inner.clone().rolling_median(options).into()
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
        };

        self.inner
            .clone()
            .rolling_quantile(quantile, interpolation.0, options)
            .into()
    }

    pub fn rolling_skew(&self, window_size: usize, bias: bool) -> Self {
        self.inner
            .clone()
            .rolling_apply_float(window_size, move |ca| {
                ca.clone().into_series().skew(bias).unwrap()
            })
            .into()
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
        self.inner
            .clone()
            .apply(
                move |s| {
                    let value = value.try_convert::<Wrap<AnyValue>>().unwrap().0;
                    s.extend_constant(value, n).map(Some)
                },
                GetOutput::same_type(),
            )
            .with_fmt("extend")
            .into()
    }

    pub fn any(&self) -> Self {
        self.inner.clone().any().into()
    }

    pub fn all(&self) -> Self {
        self.inner.clone().all().into()
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
}

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

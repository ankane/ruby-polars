use magnus::{class, RArray, RString, Value};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::conversion::*;
use crate::lazy::apply::*;
use crate::lazy::utils::rb_exprs_to_exprs;
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

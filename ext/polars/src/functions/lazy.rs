use magnus::encoding::EncodingCapable;
use magnus::{
    Float, Integer, RArray, RString, Ruby, Value, prelude::*, typed_data::Obj, value::Opaque,
};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::conversion::{Wrap, get_lf, get_rbseq};
use crate::expr::ToExprs;
use crate::expr::datatype::RbDataTypeExpr;
use crate::lazyframe::RbOptFlags;
use crate::map::lazy::binary_lambda;
use crate::utils::{EnterPolarsExt, RubyAttach};
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbPolarsErr, RbResult, RbSeries, RbValueError};

macro_rules! set_unwrapped_or_0 {
    ($($var:ident),+ $(,)?) => {
        $(let $var = $var.map(|e| e.inner.clone()).unwrap_or(polars::lazy::dsl::lit(0));)+
    };
}

pub fn rolling_corr(
    x: &RbExpr,
    y: &RbExpr,
    window_size: IdxSize,
    min_periods: IdxSize,
    ddof: u8,
) -> RbExpr {
    dsl::rolling_corr(
        x.inner.clone(),
        y.inner.clone(),
        RollingCovOptions {
            min_periods,
            window_size,
            ddof,
        },
    )
    .into()
}

pub fn rolling_cov(
    x: &RbExpr,
    y: &RbExpr,
    window_size: IdxSize,
    min_periods: IdxSize,
    ddof: u8,
) -> RbExpr {
    dsl::rolling_cov(
        x.inner.clone(),
        y.inner.clone(),
        RollingCovOptions {
            min_periods,
            window_size,
            ddof,
        },
    )
    .into()
}

pub fn arg_sort_by(
    by: RArray,
    descending: Vec<bool>,
    nulls_last: Vec<bool>,
    multithreaded: bool,
    maintain_order: bool,
) -> RbResult<RbExpr> {
    let by = by.to_exprs()?;
    Ok(dsl::arg_sort_by(
        by,
        SortMultipleOptions {
            descending,
            nulls_last,
            multithreaded,
            maintain_order,
            limit: None,
        },
    )
    .into())
}

pub fn arg_where(condition: &RbExpr) -> RbExpr {
    dsl::arg_where(condition.inner.clone()).into()
}

pub fn as_struct(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = exprs.to_exprs()?;
    if exprs.is_empty() {
        return Err(RbValueError::new_err(
            "expected at least 1 expression in 'as_struct'",
        ));
    }
    Ok(dsl::as_struct(exprs).into())
}

pub fn field(names: Vec<String>) -> RbExpr {
    dsl::Expr::Field(names.into_iter().map(|x| x.into()).collect()).into()
}

pub fn coalesce(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = exprs.to_exprs()?;
    Ok(dsl::coalesce(&exprs).into())
}

pub fn col(name: String) -> RbExpr {
    dsl::col(&name).into()
}

fn lfs_to_plans(lfs: RArray) -> RbResult<Vec<DslPlan>> {
    let lfs = lfs.typecheck::<Obj<RbLazyFrame>>()?;
    Ok(lfs
        .into_iter()
        .map(|lf| lf.ldf.read().logical_plan.clone())
        .collect())
}

pub fn collect_all(
    ruby: &Ruby,
    lfs: RArray,
    engine: Wrap<Engine>,
    optflags: &RbOptFlags,
) -> RbResult<RArray> {
    let plans = lfs_to_plans(lfs)?;
    let dfs = ruby.enter_polars(|| {
        LazyFrame::collect_all_with_engine(plans, engine.0, optflags.clone().inner.into_inner())
    })?;
    Ok(ruby.ary_from_iter(dfs.into_iter().map(Into::<RbDataFrame>::into)))
}

pub fn concat_lf(
    lfs: Value,
    rechunk: bool,
    parallel: bool,
    to_supertypes: bool,
) -> RbResult<RbLazyFrame> {
    let (seq, len) = get_rbseq(lfs)?;
    let mut lfs = Vec::with_capacity(len);

    for res in seq.into_iter() {
        let item = res;
        let lf = get_lf(item)?;
        lfs.push(lf);
    }

    let lf = dsl::concat(
        lfs,
        UnionArgs {
            rechunk,
            parallel,
            to_supertypes,
            ..Default::default()
        },
    )
    .map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

pub fn concat_list(s: RArray) -> RbResult<RbExpr> {
    let s = s.to_exprs()?;
    let expr = dsl::concat_list(s).map_err(RbPolarsErr::from)?;
    Ok(expr.into())
}

pub fn concat_arr(s: RArray) -> RbResult<RbExpr> {
    let s = s.to_exprs()?;
    let expr = dsl::concat_arr(s).map_err(RbPolarsErr::from)?;
    Ok(expr.into())
}

pub fn concat_str(s: RArray, separator: String, ignore_nulls: bool) -> RbResult<RbExpr> {
    let s = s.to_exprs()?;
    Ok(dsl::concat_str(s, &separator, ignore_nulls).into())
}

pub fn len() -> RbExpr {
    dsl::len().into()
}

pub fn cov(a: &RbExpr, b: &RbExpr, ddof: u8) -> RbExpr {
    polars::lazy::dsl::cov(a.inner.clone(), b.inner.clone(), ddof).into()
}

pub fn arctan2(y: &RbExpr, x: &RbExpr) -> RbExpr {
    y.inner.clone().arctan2(x.inner.clone()).into()
}

pub fn cum_fold(
    acc: &RbExpr,
    lambda: Value,
    exprs: RArray,
    returns_scalar: bool,
    return_dtype: Option<&RbDataTypeExpr>,
    include_init: bool,
) -> RbResult<RbExpr> {
    let exprs = exprs.to_exprs()?;
    let lambda = Opaque::from(lambda);
    let func = PlanCallback::new(move |(a, b): (Series, Series)| {
        Ruby::attach(|rb| binary_lambda(rb.get_inner(lambda), a, b).map(|v| v.unwrap()))
    });
    Ok(dsl::cum_fold_exprs(
        acc.inner.clone(),
        func,
        exprs,
        returns_scalar,
        return_dtype.map(|v| v.inner.clone()),
        include_init,
    )
    .into())
}

pub fn cum_reduce(
    lambda: Value,
    exprs: RArray,
    returns_scalar: bool,
    return_dtype: Option<&RbDataTypeExpr>,
) -> RbResult<RbExpr> {
    let exprs = exprs.to_exprs()?;
    let lambda = Opaque::from(lambda);
    let func = PlanCallback::new(move |(a, b): (Series, Series)| {
        Ruby::attach(|rb| binary_lambda(rb.get_inner(lambda), a, b).map(|v| v.unwrap()))
    });
    Ok(dsl::cum_reduce_exprs(
        func,
        exprs,
        returns_scalar,
        return_dtype.map(|v| v.inner.clone()),
    )
    .into())
}

pub fn datetime(
    year: &RbExpr,
    month: &RbExpr,
    day: &RbExpr,
    hour: Option<&RbExpr>,
    minute: Option<&RbExpr>,
    second: Option<&RbExpr>,
    microsecond: Option<&RbExpr>,
    time_unit: Wrap<TimeUnit>,
    time_zone: Wrap<Option<TimeZone>>,
    ambiguous: &RbExpr,
) -> RbExpr {
    let year = year.inner.clone();
    let month = month.inner.clone();
    let day = day.inner.clone();
    set_unwrapped_or_0!(hour, minute, second, microsecond);
    let ambiguous = ambiguous.inner.clone();
    let time_unit = time_unit.0;
    let time_zone = time_zone.0;
    let args = DatetimeArgs {
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        time_unit,
        time_zone,
        ambiguous,
    };
    dsl::datetime(args).into()
}

pub fn concat_lf_diagonal(
    lfs: RArray,
    rechunk: bool,
    parallel: bool,
    to_supertypes: bool,
) -> RbResult<RbLazyFrame> {
    let iter = lfs.into_iter();

    let lfs = iter.map(get_lf).collect::<RbResult<Vec<_>>>()?;

    let lf = dsl::functions::concat_lf_diagonal(
        lfs,
        UnionArgs {
            rechunk,
            parallel,
            to_supertypes,
            ..Default::default()
        },
    )
    .map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

pub fn concat_lf_horizontal(lfs: RArray, parallel: bool) -> RbResult<RbLazyFrame> {
    let iter = lfs.into_iter();

    let lfs = iter.map(get_lf).collect::<RbResult<Vec<_>>>()?;

    let args = UnionArgs {
        rechunk: false, // No need to rechunk with horizontal concatenation
        parallel,
        to_supertypes: false,
        ..Default::default()
    };
    let lf = dsl::functions::concat_lf_horizontal(lfs, args).map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

pub fn duration(
    weeks: Option<&RbExpr>,
    days: Option<&RbExpr>,
    hours: Option<&RbExpr>,
    minutes: Option<&RbExpr>,
    seconds: Option<&RbExpr>,
    milliseconds: Option<&RbExpr>,
    microseconds: Option<&RbExpr>,
    nanoseconds: Option<&RbExpr>,
    time_unit: Wrap<TimeUnit>,
) -> RbExpr {
    set_unwrapped_or_0!(
        weeks,
        days,
        hours,
        minutes,
        seconds,
        milliseconds,
        microseconds,
        nanoseconds,
    );
    let args = DurationArgs {
        weeks,
        days,
        hours,
        minutes,
        seconds,
        milliseconds,
        microseconds,
        nanoseconds,
        time_unit: time_unit.0,
    };
    dsl::duration(args).into()
}

pub fn fold(
    acc: &RbExpr,
    lambda: Value,
    exprs: RArray,
    returns_scalar: bool,
    return_dtype: Option<&RbDataTypeExpr>,
) -> RbResult<RbExpr> {
    let exprs = exprs.to_exprs()?;
    let lambda = Opaque::from(lambda);
    let func = PlanCallback::new(move |(a, b): (Series, Series)| {
        Ruby::attach(|rb| binary_lambda(rb.get_inner(lambda), a, b).map(|v| v.unwrap()))
    });
    Ok(dsl::fold_exprs(
        acc.inner.clone(),
        func,
        exprs,
        returns_scalar,
        return_dtype.map(|w| w.inner.clone()),
    )
    .into())
}

pub fn lit(value: Value, allow_object: bool, is_scalar: bool) -> RbResult<RbExpr> {
    let ruby = Ruby::get_with(value);
    if value.is_kind_of(ruby.class_true_class()) || value.is_kind_of(ruby.class_false_class()) {
        Ok(dsl::lit(bool::try_convert(value)?).into())
    } else if let Some(v) = Integer::from_value(value) {
        match v.to_i64() {
            Ok(val) => {
                if val > 0 && val < i32::MAX as i64 || val < 0 && val > i32::MIN as i64 {
                    Ok(dsl::lit(val as i32).into())
                } else {
                    Ok(dsl::lit(val).into())
                }
            }
            _ => {
                let val = v.to_u64()?;
                Ok(dsl::lit(val).into())
            }
        }
    } else if let Some(v) = Float::from_value(value) {
        Ok(dsl::lit(v.to_f64()).into())
    } else if let Some(v) = RString::from_value(value) {
        if v.enc_get() == ruby.utf8_encindex() {
            Ok(dsl::lit(v.to_string()?).into())
        } else {
            Ok(dsl::lit(unsafe { v.as_slice() }).into())
        }
    } else if let Ok(series) = Obj::<RbSeries>::try_convert(value) {
        let s = series.series.read();
        if is_scalar {
            let av = s
                .get(0)
                .map_err(|_| RbValueError::new_err("expected at least 1 value"))?;
            let av = av.into_static();
            Ok(dsl::lit(Scalar::new(s.dtype().clone(), av)).into())
        } else {
            Ok(dsl::lit(s.clone()).into())
        }
    } else if value.is_nil() {
        Ok(dsl::lit(Null {}).into())
    } else if allow_object {
        todo!()
    } else {
        Err(RbValueError::new_err(format!(
            "could not convert value {:?} as a Literal",
            value.to_string()
        )))
    }
}

pub fn pearson_corr(a: &RbExpr, b: &RbExpr) -> RbExpr {
    dsl::pearson_corr(a.inner.clone(), b.inner.clone()).into()
}

pub fn repeat(value: &RbExpr, n: &RbExpr, dtype: Option<Wrap<DataType>>) -> RbResult<RbExpr> {
    let mut value = value.inner.clone();
    let n = n.inner.clone();

    if let Some(dtype) = dtype {
        value = value.cast(dtype.0);
    }

    if let Expr::Literal(lv) = &value {
        let av = lv.to_any_value().unwrap();
        // Integer inputs that fit in Int32 are parsed as such
        if let DataType::Int64 = av.dtype() {
            let int_value = av.try_extract::<i64>().unwrap();
            if int_value >= i32::MIN as i64 && int_value <= i32::MAX as i64 {
                value = value.cast(DataType::Int32);
            }
        }
    }
    Ok(dsl::repeat(value, n).into())
}

pub fn spearman_rank_corr(a: &RbExpr, b: &RbExpr, propagate_nans: bool) -> RbExpr {
    dsl::spearman_rank_corr(a.inner.clone(), b.inner.clone(), propagate_nans).into()
}

pub fn sql_expr(sql: String) -> RbResult<RbExpr> {
    let expr = polars::sql::sql_expr(sql).map_err(RbPolarsErr::from)?;
    Ok(expr.into())
}

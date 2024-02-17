use magnus::encoding::{self, EncodingCapable};
use magnus::{
    class, prelude::*, typed_data::Obj, value::Opaque, Float, Integer, RArray, RString, Ruby, Value,
};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::conversion::{get_lf, get_rbseq, Wrap};
use crate::map::lazy::binary_lambda;
use crate::prelude::vec_extract_wrapped;
use crate::rb_exprs_to_exprs;
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

pub fn arg_sort_by(by: RArray, descending: Vec<bool>) -> RbResult<RbExpr> {
    let by = rb_exprs_to_exprs(by)?;
    Ok(dsl::arg_sort_by(by, &descending).into())
}

pub fn arg_where(condition: &RbExpr) -> RbExpr {
    dsl::arg_where(condition.inner.clone()).into()
}

pub fn as_struct(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::as_struct(exprs).into())
}

pub fn coalesce(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(dsl::coalesce(&exprs).into())
}

pub fn col(name: String) -> RbExpr {
    dsl::col(&name).into()
}

pub fn collect_all(lfs: RArray) -> RbResult<RArray> {
    let lfs = lfs
        .each()
        .map(|v| <&RbLazyFrame>::try_convert(v?))
        .collect::<RbResult<Vec<&RbLazyFrame>>>()?;

    Ok(RArray::from_iter(lfs.iter().map(|lf| {
        let df = lf.ldf.clone().collect().unwrap();
        RbDataFrame::new(df)
    })))
}

pub fn cols(names: Vec<String>) -> RbExpr {
    dsl::cols(names).into()
}

pub fn concat_lf(
    lfs: Value,
    rechunk: bool,
    parallel: bool,
    to_supertypes: bool,
) -> RbResult<RbLazyFrame> {
    let (seq, len) = get_rbseq(lfs)?;
    let mut lfs = Vec::with_capacity(len);

    for res in seq.each() {
        let item = res?;
        let lf = get_lf(item)?;
        lfs.push(lf);
    }

    let lf = dsl::concat(
        lfs,
        UnionArgs {
            rechunk,
            parallel,
            to_supertypes,
        },
    )
    .map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

pub fn concat_list(s: RArray) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    let expr = dsl::concat_list(s).map_err(RbPolarsErr::from)?;
    Ok(expr.into())
}

pub fn concat_str(s: RArray, separator: String, ignore_nulls: bool) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
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

pub fn arctan2d(y: &RbExpr, x: &RbExpr) -> RbExpr {
    y.inner.clone().arctan2(x.inner.clone()).degrees().into()
}

pub fn cum_fold(
    acc: &RbExpr,
    lambda: Value,
    exprs: RArray,
    include_init: bool,
) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let lambda = Opaque::from(lambda);

    let func =
        move |a: Series, b: Series| binary_lambda(Ruby::get().unwrap().get_inner(lambda), a, b);
    Ok(dsl::cum_fold_exprs(acc.inner.clone(), func, exprs, include_init).into())
}

pub fn concat_lf_diagonal(
    lfs: RArray,
    rechunk: bool,
    parallel: bool,
    to_supertypes: bool,
) -> RbResult<RbLazyFrame> {
    let iter = lfs.each();

    let lfs = iter
        .map(|item| {
            let item = item?;
            get_lf(item)
        })
        .collect::<RbResult<Vec<_>>>()?;

    let lf = dsl::functions::concat_lf_diagonal(
        lfs,
        UnionArgs {
            rechunk,
            parallel,
            to_supertypes,
        },
    )
    .map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

pub fn dtype_cols(dtypes: Vec<DataType>) -> RbExpr {
    dsl::dtype_cols(dtypes).into()
}

pub fn dtype_cols2(dtypes: RArray) -> RbResult<RbExpr> {
    let dtypes = dtypes
        .each()
        .map(|v| Wrap::<DataType>::try_convert(v?))
        .collect::<RbResult<Vec<Wrap<DataType>>>>()?;
    let dtypes = vec_extract_wrapped(dtypes);
    Ok(crate::functions::lazy::dtype_cols(dtypes))
}

#[allow(clippy::too_many_arguments)]
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

pub fn first() -> RbExpr {
    dsl::first().into()
}

pub fn fold(acc: &RbExpr, lambda: Value, exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    let lambda = Opaque::from(lambda);

    let func =
        move |a: Series, b: Series| binary_lambda(Ruby::get().unwrap().get_inner(lambda), a, b);
    Ok(dsl::fold_exprs(acc.inner.clone(), func, exprs).into())
}

pub fn last() -> RbExpr {
    dsl::last().into()
}

pub fn lit(value: Value, allow_object: bool) -> RbResult<RbExpr> {
    if value.is_kind_of(class::true_class()) || value.is_kind_of(class::false_class()) {
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
        if v.enc_get() == encoding::Index::utf8() {
            Ok(dsl::lit(v.to_string()?).into())
        } else {
            Ok(dsl::lit(unsafe { v.as_slice() }).into())
        }
    } else if let Ok(series) = Obj::<RbSeries>::try_convert(value) {
        Ok(dsl::lit(series.series.borrow().clone()).into())
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

pub fn pearson_corr(a: &RbExpr, b: &RbExpr, ddof: u8) -> RbExpr {
    dsl::pearson_corr(a.inner.clone(), b.inner.clone(), ddof).into()
}

pub fn repeat(value: &RbExpr, n: &RbExpr, dtype: Option<Wrap<DataType>>) -> RbResult<RbExpr> {
    let mut value = value.inner.clone();
    let n = n.inner.clone();

    if let Some(dtype) = dtype {
        value = value.cast(dtype.0);
    }

    if let Expr::Literal(lv) = &value {
        let av = lv.to_anyvalue().unwrap();
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

pub fn spearman_rank_corr(a: &RbExpr, b: &RbExpr, ddof: u8, propagate_nans: bool) -> RbExpr {
    dsl::spearman_rank_corr(a.inner.clone(), b.inner.clone(), ddof, propagate_nans).into()
}

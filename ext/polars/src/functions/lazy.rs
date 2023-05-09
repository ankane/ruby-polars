use magnus::{class, RArray, RString, Value};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::apply::lazy::binary_lambda;
use crate::conversion::{get_lf, get_rbseq, Wrap};
use crate::rb_exprs_to_exprs;
use crate::prelude::vec_extract_wrapped;
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbPolarsErr, RbResult, RbSeries};

macro_rules! set_unwrapped_or_0 {
    ($($var:ident),+ $(,)?) => {
        $(let $var = $var.map(|e| e.inner.clone()).unwrap_or(polars::lazy::dsl::lit(0));)+
    };
}

pub fn arange(low: &RbExpr, high: &RbExpr, step: i64) -> RbExpr {
    dsl::arange(low.inner.clone(), high.inner.clone(), step).into()
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
    Ok(dsl::as_struct(&exprs).into())
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
        .map(|v| v?.try_convert::<&RbLazyFrame>())
        .collect::<RbResult<Vec<&RbLazyFrame>>>()?;

    Ok(RArray::from_iter(lfs.iter().map(|lf| {
        let df = lf.ldf.clone().collect().unwrap();
        RbDataFrame::new(df)
    })))
}

pub fn cols(names: Vec<String>) -> RbExpr {
    dsl::cols(names).into()
}

pub fn concat_lf(lfs: Value, rechunk: bool, parallel: bool) -> RbResult<RbLazyFrame> {
    let (seq, len) = get_rbseq(lfs)?;
    let mut lfs = Vec::with_capacity(len);

    for res in seq.each() {
        let item = res?;
        let lf = get_lf(item)?;
        lfs.push(lf);
    }

    let lf = polars::lazy::dsl::concat(lfs, rechunk, parallel).map_err(RbPolarsErr::from)?;
    Ok(lf.into())
}

#[allow(clippy::too_many_arguments)]
pub fn duration(
    days: Option<&RbExpr>,
    seconds: Option<&RbExpr>,
    nanoseconds: Option<&RbExpr>,
    microseconds: Option<&RbExpr>,
    milliseconds: Option<&RbExpr>,
    minutes: Option<&RbExpr>,
    hours: Option<&RbExpr>,
    weeks: Option<&RbExpr>,
) -> RbExpr {
    set_unwrapped_or_0!(
        days,
        seconds,
        nanoseconds,
        microseconds,
        milliseconds,
        minutes,
        hours,
        weeks,
    );
    let args = DurationArgs {
        days,
        seconds,
        nanoseconds,
        microseconds,
        milliseconds,
        minutes,
        hours,
        weeks,
    };
    dsl::duration(args).into()
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

pub fn concat_str(s: RArray, sep: String) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    Ok(dsl::concat_str(s, &sep).into())
}

pub fn concat_lst(s: RArray) -> RbResult<RbExpr> {
    let s = rb_exprs_to_exprs(s)?;
    let expr = dsl::concat_list(s).map_err(RbPolarsErr::from)?;
    Ok(expr.into())
}

pub fn dtype_cols2(dtypes: RArray) -> RbResult<RbExpr> {
    let dtypes = dtypes
        .each()
        .map(|v| v?.try_convert::<Wrap<DataType>>())
        .collect::<RbResult<Vec<Wrap<DataType>>>>()?;
    let dtypes = vec_extract_wrapped(dtypes);
    Ok(crate::functions::lazy::dtype_cols(dtypes))
}

pub fn sum_exprs(exprs: RArray) -> RbResult<RbExpr> {
    let exprs = rb_exprs_to_exprs(exprs)?;
    Ok(polars::lazy::dsl::sum_exprs(exprs).into())
}

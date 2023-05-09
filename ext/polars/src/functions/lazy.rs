use magnus::{RArray, Value};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::conversion::{get_lf, get_rbseq};
use crate::rb_exprs_to_exprs;
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbPolarsErr, RbResult};

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

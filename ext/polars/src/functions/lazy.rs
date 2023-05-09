use polars::lazy::dsl;
use polars::prelude::*;
use magnus::RArray;

use crate::rb_exprs_to_exprs;
use crate::{RbExpr, RbResult};

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

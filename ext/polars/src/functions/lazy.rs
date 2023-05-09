use polars::lazy::dsl;
use polars::prelude::*;

use crate::{RbExpr};

macro_rules! set_unwrapped_or_0 {
    ($($var:ident),+ $(,)?) => {
        $(let $var = $var.map(|e| e.inner.clone()).unwrap_or(polars::lazy::dsl::lit(0));)+
    };
}

pub fn arange(low: &RbExpr, high: &RbExpr, step: i64) -> RbExpr {
    dsl::arange(low.inner.clone(), high.inner.clone(), step).into()
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

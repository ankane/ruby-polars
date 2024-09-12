use polars::lazy::dsl;
use polars_core::datatypes::{TimeUnit, TimeZone};

use crate::conversion::Wrap;
use crate::prelude::*;
use crate::RbExpr;

pub fn int_range(start: &RbExpr, end: &RbExpr, step: i64, dtype: Wrap<DataType>) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let dtype = dtype.0;
    dsl::int_range(start, end, step, dtype).into()
}

pub fn int_ranges(start: &RbExpr, end: &RbExpr, step: &RbExpr, dtype: Wrap<DataType>) -> RbExpr {
    let dtype = dtype.0;

    let mut result = dsl::int_ranges(start.inner.clone(), end.inner.clone(), step.inner.clone());

    if dtype != DataType::Int64 {
        result = result.cast(DataType::List(Box::new(dtype)))
    }

    result.into()
}

pub fn date_range(
    start: &RbExpr,
    end: &RbExpr,
    interval: String,
    closed: Wrap<ClosedWindow>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let interval = Duration::parse(&interval);
    let closed = closed.0;
    dsl::date_range(start, end, interval, closed).into()
}

pub fn date_ranges(
    start: &RbExpr,
    end: &RbExpr,
    interval: String,
    closed: Wrap<ClosedWindow>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let interval = Duration::parse(&interval);
    let closed = closed.0;
    dsl::date_ranges(start, end, interval, closed).into()
}

pub fn datetime_range(
    start: &RbExpr,
    end: &RbExpr,
    every: String,
    closed: Wrap<ClosedWindow>,
    time_unit: Option<Wrap<TimeUnit>>,
    time_zone: Option<Wrap<TimeZone>>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let every = Duration::parse(&every);
    let closed = closed.0;
    let time_unit = time_unit.map(|x| x.0);
    let time_zone = time_zone.map(|x| x.0);
    dsl::datetime_range(start, end, every, closed, time_unit, time_zone).into()
}

pub fn datetime_ranges(
    start: &RbExpr,
    end: &RbExpr,
    every: String,
    closed: Wrap<ClosedWindow>,
    time_unit: Option<Wrap<TimeUnit>>,
    time_zone: Option<Wrap<TimeZone>>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let every = Duration::parse(&every);
    let closed = closed.0;
    let time_unit = time_unit.map(|x| x.0);
    let time_zone = time_zone.map(|x| x.0);
    dsl::datetime_ranges(start, end, every, closed, time_unit, time_zone).into()
}

pub fn time_range(
    start: &RbExpr,
    end: &RbExpr,
    every: String,
    closed: Wrap<ClosedWindow>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let every = Duration::parse(&every);
    let closed = closed.0;
    dsl::time_range(start, end, every, closed).into()
}

pub fn time_ranges(
    start: &RbExpr,
    end: &RbExpr,
    every: String,
    closed: Wrap<ClosedWindow>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    let every = Duration::parse(&every);
    let closed = closed.0;
    dsl::time_ranges(start, end, every, closed).into()
}

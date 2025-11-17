mod aggregation;
mod arithmetic;
mod comparison;
mod construction;
mod export;
mod general;
mod import;
mod map;
mod scatter;

pub(crate) use import::import_schema_rbcapsule;

use magnus::{DataTypeFunctions, RArray, Ruby, TypedData, gc, prelude::*};
use parking_lot::RwLock;
use polars::prelude::*;

use crate::{ObjectValue, RbResult};

#[derive(TypedData)]
#[magnus(class = "Polars::RbSeries", mark)]
pub struct RbSeries {
    pub series: RwLock<Series>,
}

impl Clone for RbSeries {
    fn clone(&self) -> Self {
        Self {
            series: RwLock::new(self.series.read().clone()),
        }
    }
}

impl From<Series> for RbSeries {
    fn from(series: Series) -> Self {
        Self::new(series)
    }
}

impl RbSeries {
    pub fn new(series: Series) -> Self {
        RbSeries {
            series: RwLock::new(series),
        }
    }
}

pub fn to_series(rs: RArray) -> RbResult<Vec<Series>> {
    let mut series = Vec::new();
    for item in rs.into_iter() {
        series.push(<&RbSeries>::try_convert(item)?.series.read().clone());
    }
    Ok(series)
}

pub(crate) trait ToRbSeries {
    fn to_rbseries(self, rb: &Ruby) -> RArray;
}

impl ToRbSeries for Vec<Column> {
    fn to_rbseries(self, rb: &Ruby) -> RArray {
        rb.ary_from_iter(
            self.into_iter()
                .map(|c| c.take_materialized_series())
                .map(RbSeries::new),
        )
    }
}

pub fn mark_series(marker: &gc::Marker, series: &Series) {
    if let DataType::Object(_) = series.dtype() {
        for i in 0..series.len() {
            let obj: Option<&ObjectValue> = series.get_object(i).map(|any| any.into());
            if let Some(o) = obj {
                marker.mark(o.inner);
            }
        }
    }
}

impl DataTypeFunctions for RbSeries {
    fn mark(&self, marker: &gc::Marker) {
        // this is not ideal, as objects will not be marked if unable to borrow
        // this should never happen, but log for now to avoid panic,
        // as most series will not use Object datatype
        if let Some(s) = &self.series.try_read() {
            mark_series(marker, s);
        } else {
            eprintln!("[polars] Could not borrow!");
        }
    }
}

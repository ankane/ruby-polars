mod aggregation;
mod arithmetic;
mod comparison;
mod construction;
mod export;
mod general;
mod import;
mod map;
mod scatter;

use magnus::{DataTypeFunctions, RArray, Ruby, TypedData, gc, prelude::*};
use polars::prelude::*;
use std::cell::RefCell;

use crate::{ObjectValue, RbResult};

#[derive(TypedData)]
#[magnus(class = "Polars::RbSeries", mark)]
pub struct RbSeries {
    pub series: RefCell<Series>,
}

impl From<Series> for RbSeries {
    fn from(series: Series) -> Self {
        RbSeries::new(series)
    }
}

impl RbSeries {
    pub fn new(series: Series) -> Self {
        RbSeries {
            series: RefCell::new(series),
        }
    }
}

pub fn to_series(rs: RArray) -> RbResult<Vec<Series>> {
    let mut series = Vec::new();
    for item in rs.into_iter() {
        series.push(<&RbSeries>::try_convert(item)?.series.borrow().clone());
    }
    Ok(series)
}

pub fn to_rbseries(s: Vec<Column>) -> RArray {
    Ruby::get().unwrap().ary_from_iter(
        s.into_iter()
            .map(|c| c.take_materialized_series())
            .map(RbSeries::new),
    )
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
        mark_series(marker, &self.series.borrow());
    }
}

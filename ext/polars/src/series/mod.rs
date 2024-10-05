mod aggregation;
mod arithmetic;
mod comparison;
mod construction;
mod export;
mod general;
mod scatter;

use magnus::{prelude::*, RArray};
use polars::prelude::*;
use std::cell::RefCell;

use crate::RbResult;

#[magnus::wrap(class = "Polars::RbSeries")]
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

pub fn to_series_collection(rs: RArray) -> RbResult<Vec<Series>> {
    let mut series = Vec::new();
    for item in rs.into_iter() {
        series.push(<&RbSeries>::try_convert(item)?.series.borrow().clone());
    }
    Ok(series)
}

pub fn to_rbseries_collection(s: Vec<Series>) -> RArray {
    RArray::from_iter(s.into_iter().map(RbSeries::new))
}

mod construction;
mod export;
mod general;
mod io;
mod serde;

use magnus::{DataTypeFunctions, TypedData, gc};
use polars::prelude::*;
use std::cell::RefCell;

use crate::series::mark_series;

#[derive(TypedData)]
#[magnus(class = "Polars::RbDataFrame", mark)]
pub struct RbDataFrame {
    pub df: RefCell<DataFrame>,
}

impl From<DataFrame> for RbDataFrame {
    fn from(df: DataFrame) -> Self {
        RbDataFrame::new(df)
    }
}

impl RbDataFrame {
    pub fn new(df: DataFrame) -> Self {
        RbDataFrame {
            df: RefCell::new(df),
        }
    }
}

impl DataTypeFunctions for RbDataFrame {
    fn mark(&self, marker: &gc::Marker) {
        for column in self.df.borrow().get_columns() {
            if let DataType::Object(_) = column.dtype() {
                match column {
                    Column::Series(s) => mark_series(marker, s),
                    Column::Partitioned(s) => mark_series(marker, s.partitions()),
                    Column::Scalar(s) => mark_series(marker, &s.as_single_value_series()),
                }
            }
        }
    }
}

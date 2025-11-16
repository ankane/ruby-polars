mod construction;
mod export;
mod general;
mod io;
mod serde;

use magnus::{DataTypeFunctions, TypedData, gc};
use parking_lot::RwLock;
use polars::prelude::*;

use crate::series::mark_series;

#[derive(TypedData)]
#[magnus(class = "Polars::RbDataFrame", mark)]
pub struct RbDataFrame {
    pub df: RwLock<DataFrame>,
}

impl From<DataFrame> for RbDataFrame {
    fn from(df: DataFrame) -> Self {
        Self::new(df)
    }
}

impl RbDataFrame {
    pub fn new(df: DataFrame) -> Self {
        RbDataFrame {
            df: RwLock::new(df),
        }
    }
}

impl DataTypeFunctions for RbDataFrame {
    fn mark(&self, marker: &gc::Marker) {
        // this is really, really not ideal, as objects will not be marked if unable to borrow
        // currently, this should only happen for write_* methods,
        // which should refuse to write Object datatype, and therefore be safe,
        // since GC will not have a chance to run
        if let Some(df) = self.df.try_read() {
            for column in df.get_columns() {
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
}

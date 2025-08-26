use arrow::datatypes::ArrowDataType;
use arrow::ffi;
use magnus::{IntoValue, Ruby, Value};
use polars::datatypes::CompatLevel;
use polars::frame::DataFrame;
use polars::prelude::{ArrayRef, ArrowField, PlSmallStr, PolarsResult, SchemaExt};
use polars::series::Series;
use polars_core::utils::arrow;

use crate::RbResult;

#[magnus::wrap(class = "Polars::ArrowArrayStream")]
pub struct RbArrowArrayStream {
    stream: ffi::ArrowArrayStream,
}

impl RbArrowArrayStream {
    pub fn to_i(&self) -> usize {
        (&self.stream as *const _) as usize
    }
}

pub(crate) fn dataframe_to_stream(df: &DataFrame, ruby: &Ruby) -> RbResult<Value> {
    let iter = Box::new(DataFrameStreamIterator::new(df));
    let field = iter.field();
    let stream = ffi::export_iterator(iter, field);
    Ok(RbArrowArrayStream { stream }.into_value_with(ruby))
}

pub struct DataFrameStreamIterator {
    columns: Vec<Series>,
    dtype: ArrowDataType,
    idx: usize,
    n_chunks: usize,
}

impl DataFrameStreamIterator {
    fn new(df: &DataFrame) -> Self {
        let schema = df.schema().to_arrow(CompatLevel::newest());
        let dtype = ArrowDataType::Struct(schema.into_iter_values().collect());

        Self {
            columns: df
                .get_columns()
                .iter()
                .map(|v| v.as_materialized_series().clone())
                .collect(),
            dtype,
            idx: 0,
            n_chunks: df.first_col_n_chunks(),
        }
    }

    fn field(&self) -> ArrowField {
        ArrowField::new(PlSmallStr::EMPTY, self.dtype.clone(), false)
    }
}

impl Iterator for DataFrameStreamIterator {
    type Item = PolarsResult<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.n_chunks {
            None
        } else {
            // create a batch of the columns with the same chunk no.
            let batch_cols = self
                .columns
                .iter()
                .map(|s| s.to_arrow(self.idx, CompatLevel::newest()))
                .collect::<Vec<_>>();
            self.idx += 1;

            let array = arrow::array::StructArray::new(
                self.dtype.clone(),
                batch_cols[0].len(),
                batch_cols,
                None,
            );
            Some(Ok(Box::new(array)))
        }
    }
}

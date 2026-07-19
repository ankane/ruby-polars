use arrow::datatypes::ArrowDataType;
use arrow::ffi;
use magnus::{IntoValue, Ruby, Value};
use polars::datatypes::CompatLevel;
use polars::frame::DataFrame;
use polars::prelude::{ArrayRef, ArrowField, PlSmallStr, PolarsResult, SchemaExt};
use polars::series::Series;
use polars_core::utils::arrow;
use std::ffi::CString;

use crate::RbResult;
use crate::ruby::capsule::RbCapsule;

// TODO switch to RbCapsule in 0.27.0
#[magnus::wrap(class = "Polars::ArrowSchema")]
pub struct RbArrowSchema {
    pub(crate) schema: ffi::ArrowSchema,
}

impl RbArrowSchema {
    pub fn to_i(&self) -> usize {
        (&self.schema as *const _) as usize
    }
}

pub(crate) fn series_to_stream(series: &Series, ruby: &Ruby) -> RbResult<Value> {
    let field = series.field().to_arrow(CompatLevel::newest());
    let series = series.clone();
    let iter = Box::new(
        (0..series.n_chunks()).map(move |i| Ok(series.to_arrow(i, CompatLevel::newest()))),
    ) as _;

    let stream = ffi::export_iterator(iter, field);
    let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
    Ok(RbCapsule::new(stream, Some(stream_capsule_name)).into_value_with(ruby))
}

pub(crate) fn dataframe_to_stream(df: &DataFrame, ruby: &Ruby) -> RbResult<Value> {
    let iter = Box::new(DataFrameStreamIterator::new(df));
    let field = iter.field();
    let stream = ffi::export_iterator(iter, field);
    let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
    Ok(RbCapsule::new(stream, Some(stream_capsule_name)).into_value_with(ruby))
}

pub(crate) fn polars_schema_to_rbcapsule(
    ruby: &Ruby,
    schema: crate::prelude::Wrap<polars::prelude::Schema>,
) -> RbResult<Value> {
    let schema: arrow::ffi::ArrowSchema = arrow::ffi::export_field_to_c(&ArrowField::new(
        PlSmallStr::EMPTY,
        ArrowDataType::Struct(
            schema
                .0
                .iter_fields()
                .map(|x| x.to_arrow(CompatLevel::newest()))
                .collect(),
        ),
        false,
    ));

    Ok(RbArrowSchema { schema }.into_value_with(ruby))
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
                .columns()
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

use magnus::{prelude::*, r_hash::ForEach, RArray, RHash, Value};
use polars::frame::row::{rows_to_schema_supertypes, Row};
use polars::prelude::*;

use super::*;
use crate::conversion::*;
use crate::rb_modules;
use crate::{RbPolarsErr, RbResult, RbSeries};

impl RbDataFrame {
    pub fn from_rows(
        rb_rows: RArray,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let mut rows = Vec::with_capacity(rb_rows.len());
        for v in rb_rows.each() {
            let rb_row = RArray::try_convert(v?)?;
            let mut row = Vec::with_capacity(rb_row.len());
            for val in rb_row.each() {
                row.push(Wrap::<AnyValue>::try_convert(val?)?.0);
            }
            rows.push(Row(row));
        }
        finish_from_rows(rows, infer_schema_length, schema.map(|wrap| wrap.0), None)
    }

    pub fn from_hashes(
        dicts: Value,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
        schema_overrides: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let mut schema_columns = PlIndexSet::new();
        if let Some(s) = &schema {
            schema_columns.extend(s.0.iter_names().map(|n| n.to_string()))
        }
        let (rows, names) = dicts_to_rows(&dicts, infer_schema_length, schema_columns)?;

        let mut schema_overrides_by_idx: Vec<(usize, DataType)> = Vec::new();
        if let Some(overrides) = schema_overrides {
            for (idx, name) in names.iter().enumerate() {
                if let Some(dtype) = overrides.0.get(name) {
                    schema_overrides_by_idx.push((idx, dtype.clone()));
                }
            }
        }
        let rbdf = finish_from_rows(
            rows,
            infer_schema_length,
            schema.map(|wrap| wrap.0),
            Some(schema_overrides_by_idx),
        )?;

        unsafe {
            rbdf.df
                .borrow_mut()
                .get_columns_mut()
                .iter_mut()
                .zip(&names)
                .for_each(|(s, name)| {
                    s.rename(name);
                });
        }
        let length = names.len();
        if names.into_iter().collect::<PlHashSet<_>>().len() != length {
            let err = PolarsError::SchemaMismatch("duplicate column names found".into());
            Err(RbPolarsErr::from(err))?;
        }

        Ok(rbdf)
    }

    pub fn read_hash(data: RHash) -> RbResult<Self> {
        let mut cols: Vec<Series> = Vec::new();
        data.foreach(|name: String, values: Value| {
            let obj: Value = rb_modules::series().funcall("new", (name, values))?;
            let rbseries = obj.funcall::<_, _, &RbSeries>("_s", ())?;
            cols.push(rbseries.series.borrow().clone());
            Ok(ForEach::Continue)
        })?;
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }
}

fn finish_from_rows(
    rows: Vec<Row>,
    infer_schema_length: Option<usize>,
    schema: Option<Schema>,
    schema_overrides_by_idx: Option<Vec<(usize, DataType)>>,
) -> RbResult<RbDataFrame> {
    // Object builder must be registered
    crate::on_startup::register_object_builder();

    let mut final_schema =
        rows_to_schema_supertypes(&rows, infer_schema_length.map(|n| std::cmp::max(1, n)))
            .map_err(RbPolarsErr::from)?;

    // Erase scale from inferred decimals.
    for dtype in final_schema.iter_dtypes_mut() {
        if let DataType::Decimal(_, _) = dtype {
            *dtype = DataType::Decimal(None, None)
        }
    }

    // Integrate explicit/inferred schema.
    if let Some(schema) = schema {
        for (i, (name, dtype)) in schema.into_iter().enumerate() {
            if let Some((name_, dtype_)) = final_schema.get_at_index_mut(i) {
                *name_ = name;

                // If schema dtype is Unknown, overwrite with inferred datatype.
                if !matches!(dtype, DataType::Unknown(_)) {
                    *dtype_ = dtype;
                }
            } else {
                final_schema.with_column(name, dtype);
            }
        }
    }

    // Optional per-field overrides; these supersede default/inferred dtypes.
    if let Some(overrides) = schema_overrides_by_idx {
        for (i, dtype) in overrides {
            if let Some((_, dtype_)) = final_schema.get_at_index_mut(i) {
                if !matches!(dtype, DataType::Unknown(_)) {
                    *dtype_ = dtype;
                }
            }
        }
    }
    let df = DataFrame::from_rows_and_schema(&rows, &final_schema).map_err(RbPolarsErr::from)?;
    Ok(df.into())
}

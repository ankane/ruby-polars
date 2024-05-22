use magnus::{prelude::*, r_hash::ForEach, RArray, RHash, Symbol, Value};
use polars::frame::row::{rows_to_schema_supertypes, rows_to_supertypes, Row};
use polars::prelude::*;

use super::*;
use crate::conversion::*;
use crate::{RbPolarsErr, RbResult};

impl RbDataFrame {
    pub fn from_rows(
        rb_rows: RArray,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let mut data = Vec::with_capacity(rb_rows.len());
        for v in rb_rows.each() {
            let rb_row = RArray::try_convert(v?)?;
            let mut row = Vec::with_capacity(rb_row.len());
            for val in rb_row.each() {
                row.push(Wrap::<AnyValue>::try_convert(val?)?.0);
            }
            data.push(Row(row));
        }
        finish_from_rows(data, infer_schema_length, schema.map(|wrap| wrap.0), None)
    }

    pub fn from_hashes(
        dicts: Value,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
        schema_overrides: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let schema_overrides = schema_overrides.map(|wrap| wrap.0);

        let mut schema_columns = PlIndexSet::new();
        if let Some(s) = &schema {
            schema_columns.extend(s.0.iter_names().map(|n| n.to_string()))
        }
        let (rows, names) = dicts_to_rows(&dicts, infer_schema_length, schema_columns)?;

        let rbdf = finish_from_rows(
            rows,
            infer_schema_length,
            schema.map(|wrap| wrap.0),
            schema_overrides,
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
}

fn finish_from_rows(
    rows: Vec<Row>,
    infer_schema_length: Option<usize>,
    schema: Option<Schema>,
    schema_overrides: Option<Schema>,
) -> RbResult<RbDataFrame> {
    // Object builder must be registered
    crate::on_startup::register_object_builder();

    let mut schema = if let Some(mut schema) = schema {
        resolve_schema_overrides(&mut schema, schema_overrides);
        update_schema_from_rows(&mut schema, &rows, infer_schema_length)?;
        schema
    } else {
        rows_to_schema_supertypes(&rows, infer_schema_length).map_err(RbPolarsErr::from)?
    };

    // TODO: Remove this step when Decimals are supported properly.
    // Erasing the decimal precision/scale here will just require us to infer it again later.
    // https://github.com/pola-rs/polars/issues/14427
    erase_decimal_precision_scale(&mut schema);

    let df = DataFrame::from_rows_and_schema(&rows, &schema).map_err(RbPolarsErr::from)?;
    Ok(df.into())
}

fn update_schema_from_rows(
    schema: &mut Schema,
    rows: &[Row],
    infer_schema_length: Option<usize>,
) -> RbResult<()> {
    let schema_is_complete = schema.iter_dtypes().all(|dtype| dtype.is_known());
    if schema_is_complete {
        return Ok(());
    }

    // TODO: Only infer dtypes for columns with an unknown dtype
    let inferred_dtypes =
        rows_to_supertypes(rows, infer_schema_length).map_err(RbPolarsErr::from)?;
    let inferred_dtypes_slice = inferred_dtypes.as_slice();

    for (i, dtype) in schema.iter_dtypes_mut().enumerate() {
        if !dtype.is_known() {
            *dtype = inferred_dtypes_slice.get(i).ok_or_else(|| {
                polars_err!(SchemaMismatch: "the number of columns in the schema does not match the data")
            })
            .map_err(RbPolarsErr::from)?
            .clone();
        }
    }
    Ok(())
}

fn resolve_schema_overrides(schema: &mut Schema, schema_overrides: Option<Schema>) {
    if let Some(overrides) = schema_overrides {
        for (name, dtype) in overrides.into_iter() {
            schema.set_dtype(name.as_str(), dtype);
        }
    }
}

fn erase_decimal_precision_scale(schema: &mut Schema) {
    for dtype in schema.iter_dtypes_mut() {
        if let DataType::Decimal(_, _) = dtype {
            *dtype = DataType::Decimal(None, None)
        }
    }
}

fn dicts_to_rows(
    records: &Value,
    infer_schema_len: Option<usize>,
    schema_columns: PlIndexSet<String>,
) -> RbResult<(Vec<Row>, Vec<String>)> {
    let infer_schema_len = infer_schema_len.map(|n| std::cmp::max(1, n));
    let (dicts, len) = get_rbseq(*records)?;

    let key_names = {
        if !schema_columns.is_empty() {
            schema_columns
        } else {
            let mut inferred_keys = PlIndexSet::new();
            for d in dicts.each().take(infer_schema_len.unwrap_or(usize::MAX)) {
                let d = d?;
                let d = RHash::try_convert(d)?;

                d.foreach(|name: Value, _value: Value| {
                    if let Some(v) = Symbol::from_value(name) {
                        inferred_keys.insert(v.name()?.into());
                    } else {
                        inferred_keys.insert(String::try_convert(name)?);
                    };
                    Ok(ForEach::Continue)
                })?;
            }
            inferred_keys
        }
    };

    let mut rows = Vec::with_capacity(len);

    for d in dicts.each() {
        let d = d?;
        let d = RHash::try_convert(d)?;

        let mut row = Vec::with_capacity(key_names.len());

        for k in key_names.iter() {
            // TODO improve performance
            let val = match d.get(k.clone()).or_else(|| d.get(Symbol::new(k))) {
                None => AnyValue::Null,
                Some(val) => Wrap::<AnyValue>::try_convert(val)?.0,
            };
            row.push(val)
        }
        rows.push(Row(row))
    }
    Ok((rows, key_names.into_iter().collect()))
}

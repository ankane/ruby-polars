use magnus::{RArray, RHash, Symbol, Value, prelude::*, r_hash::ForEach};
use polars::frame::row::{Row, rows_to_schema_supertypes, rows_to_supertypes};
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
        for v in rb_rows.into_iter() {
            let rb_row = RArray::try_convert(v)?;
            let mut row = Vec::with_capacity(rb_row.len());
            for val in rb_row.into_iter() {
                row.push(Wrap::<AnyValue>::try_convert(val)?.0);
            }
            data.push(Row(row));
        }
        let schema = schema.map(|wrap| wrap.0);
        finish_from_rows(data, schema, None, infer_schema_length)
    }

    pub fn from_hashes(
        data: Value,
        schema: Option<Wrap<Schema>>,
        schema_overrides: Option<Wrap<Schema>>,
        strict: bool,
        infer_schema_length: Option<usize>,
    ) -> RbResult<Self> {
        let schema = schema.map(|wrap| wrap.0);
        let schema_overrides = schema_overrides.map(|wrap| wrap.0);

        let names = get_schema_names(&data, schema.as_ref(), infer_schema_length)?;
        let rows = dicts_to_rows(&data, &names, strict)?;

        let schema = schema.or_else(|| {
            Some(columns_names_to_empty_schema(
                names.iter().map(String::as_str),
            ))
        });

        finish_from_rows(rows, schema, schema_overrides, infer_schema_length)
    }
}

fn finish_from_rows(
    rows: Vec<Row>,
    schema: Option<Schema>,
    schema_overrides: Option<Schema>,
    infer_schema_length: Option<usize>,
) -> RbResult<RbDataFrame> {
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
    let schema_is_complete = schema.iter_values().all(|dtype| dtype.is_known());
    if schema_is_complete {
        return Ok(());
    }

    // TODO: Only infer dtypes for columns with an unknown dtype
    let inferred_dtypes =
        rows_to_supertypes(rows, infer_schema_length).map_err(RbPolarsErr::from)?;
    let inferred_dtypes_slice = inferred_dtypes.as_slice();

    for (i, dtype) in schema.iter_values_mut().enumerate() {
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
    for dtype in schema.iter_values_mut() {
        if let DataType::Decimal(_, _) = dtype {
            *dtype = DataType::Decimal(None, None)
        }
    }
}

fn columns_names_to_empty_schema<'a, I>(column_names: I) -> Schema
where
    I: IntoIterator<Item = &'a str>,
{
    let fields = column_names
        .into_iter()
        .map(|c| Field::new(c.into(), DataType::Unknown(Default::default())));
    Schema::from_iter(fields)
}

fn dicts_to_rows<'a>(data: &Value, names: &'a [String], _strict: bool) -> RbResult<Vec<Row<'a>>> {
    let (data, len) = get_rbseq(*data)?;
    let mut rows = Vec::with_capacity(len);
    for d in data.into_iter() {
        let d = RHash::try_convert(d)?;

        let mut row = Vec::with_capacity(names.len());
        for k in names.iter() {
            // TODO improve performance
            let val = match d.get(k.clone()).or_else(|| d.get(Symbol::new(k))) {
                None => AnyValue::Null,
                Some(val) => Wrap::<AnyValue>::try_convert(val)?.0,
            };
            row.push(val)
        }
        rows.push(Row(row))
    }
    Ok(rows)
}

fn get_schema_names(
    data: &Value,
    schema: Option<&Schema>,
    infer_schema_length: Option<usize>,
) -> RbResult<Vec<String>> {
    if let Some(schema) = schema {
        Ok(schema.iter_names().map(|n| n.to_string()).collect())
    } else {
        infer_schema_names_from_data(data, infer_schema_length)
    }
}

fn infer_schema_names_from_data(
    data: &Value,
    infer_schema_length: Option<usize>,
) -> RbResult<Vec<String>> {
    let (data, data_len) = get_rbseq(*data)?;
    let infer_schema_length = infer_schema_length
        .map(|n| std::cmp::max(1, n))
        .unwrap_or(data_len);

    let mut names = PlIndexSet::new();
    for d in data.into_iter().take(infer_schema_length) {
        let d = RHash::try_convert(d)?;
        d.foreach(|name: Value, _value: Value| {
            if let Some(v) = Symbol::from_value(name) {
                names.insert(v.name()?.into());
            } else {
                names.insert(String::try_convert(name)?);
            };
            Ok(ForEach::Continue)
        })?;
    }
    Ok(names.into_iter().collect())
}

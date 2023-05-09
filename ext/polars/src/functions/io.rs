use magnus::{RHash, Value};

use crate::conversion::Wrap;
use crate::file::get_file_like;
use crate::prelude::DataType;
use crate::{RbPolarsErr, RbResult};

pub fn read_ipc_schema(rb_f: Value) -> RbResult<Value> {
    use polars_core::export::arrow::io::ipc::read::read_file_metadata;
    let mut r = get_file_like(rb_f, false)?;
    let metadata = read_file_metadata(&mut r).map_err(RbPolarsErr::arrow)?;

    let dict = RHash::new();
    for field in metadata.schema.fields {
        let dt: Wrap<DataType> = Wrap((&field.data_type).into());
        dict.aset(field.name, dt)?;
    }
    Ok(dict.into())
}

pub fn read_parquet_schema(rb_f: Value) -> RbResult<Value> {
    use polars_core::export::arrow::io::parquet::read::{infer_schema, read_metadata};

    let mut r = get_file_like(rb_f, false)?;
    let metadata = read_metadata(&mut r).map_err(RbPolarsErr::arrow)?;
    let arrow_schema = infer_schema(&metadata).map_err(RbPolarsErr::arrow)?;

    let dict = RHash::new();
    for field in arrow_schema.fields {
        let dt: Wrap<DataType> = Wrap((&field.data_type).into());
        dict.aset(field.name, dt)?;
    }
    Ok(dict.into())
}

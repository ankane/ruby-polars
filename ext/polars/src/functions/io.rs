use magnus::{RHash, Value};

use crate::conversion::Wrap;
use crate::file::get_file_like;
use crate::prelude::DataType;
use crate::{RbPolarsErr, RbResult};

pub fn read_ipc_schema(rb_f: Value) -> RbResult<RHash> {
    use polars_core::export::arrow::io::ipc::read::read_file_metadata;
    let mut r = get_file_like(rb_f, false)?;
    let metadata = read_file_metadata(&mut r).map_err(RbPolarsErr::from)?;

    let dict = RHash::new();
    for field in metadata.schema.iter_values() {
        let dt: Wrap<DataType> = Wrap((&field.dtype).into());
        dict.aset(field.name.as_str(), dt)?;
    }
    Ok(dict)
}

pub fn read_parquet_schema(rb_f: Value) -> RbResult<RHash> {
    use polars_parquet::read::{infer_schema, read_metadata};

    let mut r = get_file_like(rb_f, false)?;
    let metadata = read_metadata(&mut r).map_err(RbPolarsErr::from)?;
    let arrow_schema = infer_schema(&metadata).map_err(RbPolarsErr::from)?;

    let dict = RHash::new();
    for field in arrow_schema.iter_values() {
        let dt: Wrap<DataType> = Wrap((&field.dtype).into());
        dict.aset(field.name.as_str(), dt)?;
    }
    Ok(dict)
}

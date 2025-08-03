use std::io::BufReader;

use magnus::{RHash, Value};
use polars::prelude::ArrowSchema;

use crate::conversion::Wrap;
use crate::file::{EitherRustRubyFile, get_either_file};
use crate::{RbPolarsErr, RbResult};

pub fn read_ipc_schema(rb_f: Value) -> RbResult<RHash> {
    use arrow::io::ipc::read::read_file_metadata;
    let metadata = match get_either_file(rb_f, false)? {
        EitherRustRubyFile::Rust(r) => {
            read_file_metadata(&mut BufReader::new(r)).map_err(RbPolarsErr::from)?
        }
        EitherRustRubyFile::Rb(mut r) => read_file_metadata(&mut r).map_err(RbPolarsErr::from)?,
    };

    let dict = RHash::new();
    fields_to_rbdict(&metadata.schema, &dict)?;
    Ok(dict)
}

pub fn read_parquet_schema(rb_f: Value) -> RbResult<RHash> {
    use polars_parquet::read::{infer_schema, read_metadata};

    let metadata = match get_either_file(rb_f, false)? {
        EitherRustRubyFile::Rust(r) => {
            read_metadata(&mut BufReader::new(r)).map_err(RbPolarsErr::from)?
        }
        EitherRustRubyFile::Rb(mut r) => read_metadata(&mut r).map_err(RbPolarsErr::from)?,
    };
    let arrow_schema = infer_schema(&metadata).map_err(RbPolarsErr::from)?;

    let dict = RHash::new();
    fields_to_rbdict(&arrow_schema, &dict)?;
    Ok(dict)
}

fn fields_to_rbdict(schema: &ArrowSchema, dict: &RHash) -> RbResult<()> {
    for field in schema.iter_values() {
        let dt = Wrap(polars::prelude::DataType::from_arrow_field(field));
        dict.aset(field.name.as_str(), dt)?;
    }
    Ok(())
}

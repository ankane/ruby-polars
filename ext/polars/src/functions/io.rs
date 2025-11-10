use std::io::BufReader;

use magnus::{RHash, Ruby, Value};
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

    let ruby = Ruby::get_with(rb_f);
    let dict = ruby.hash_new();
    fields_to_rbdict(&metadata.schema, &dict)?;
    Ok(dict)
}

pub fn read_parquet_metadata(
    rb_f: Value,
    _storage_options: Option<Vec<(String, String)>>,
    _credential_provider: Option<Value>,
    _retries: usize,
) -> RbResult<RHash> {
    use polars_parquet::read::read_metadata;
    use polars_parquet::read::schema::read_custom_key_value_metadata;

    let metadata = match get_either_file(rb_f, false)? {
        EitherRustRubyFile::Rust(r) => {
            read_metadata(&mut BufReader::new(r)).map_err(RbPolarsErr::from)?
        }
        EitherRustRubyFile::Rb(mut r) => read_metadata(&mut r).map_err(RbPolarsErr::from)?,
    };

    let key_value_metadata = read_custom_key_value_metadata(metadata.key_value_metadata());
    let ruby = Ruby::get_with(rb_f);
    let dict = ruby.hash_new();
    for (key, value) in key_value_metadata.into_iter() {
        dict.aset(key.as_str(), value.as_str())?;
    }
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

    let ruby = Ruby::get_with(rb_f);
    let dict = ruby.hash_new();
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

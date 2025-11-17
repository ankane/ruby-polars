use std::io::BufReader;

use magnus::{RHash, Ruby, Value};
use polars::prelude::ArrowSchema;
use polars::prelude::PlPathRef;
use polars_io::cloud::CloudOptions;

use crate::conversion::Wrap;
use crate::file::{EitherRustRubyFile, get_either_file};
use crate::utils::EnterPolarsExt;
use crate::{RbPolarsErr, RbResult};

pub fn read_ipc_schema(rb: &Ruby, rb_f: Value) -> RbResult<RHash> {
    use arrow::io::ipc::read::read_file_metadata;
    let metadata = match get_either_file(rb_f, false)? {
        EitherRustRubyFile::Rust(r) => {
            read_file_metadata(&mut BufReader::new(r)).map_err(RbPolarsErr::from)?
        }
        EitherRustRubyFile::Rb(mut r) => read_file_metadata(&mut r).map_err(RbPolarsErr::from)?,
    };

    let dict = rb.hash_new();
    fields_to_rbdict(&metadata.schema, &dict)?;
    Ok(dict)
}

pub fn read_parquet_metadata(
    rb: &Ruby,
    rb_f: Value,
    storage_options: Option<Vec<(String, String)>>,
    credential_provider: Option<Value>,
    retries: usize,
) -> RbResult<RHash> {
    use std::io::Cursor;

    use polars_io::pl_async::get_runtime;
    use polars_parquet::read::read_metadata;
    use polars_parquet::read::schema::read_custom_key_value_metadata;
    use polars_utils::plpath::PlPath;

    use crate::file::{RubyScanSourceInput, get_ruby_scan_source_input};

    let metadata = match get_ruby_scan_source_input(rb_f, false)? {
        RubyScanSourceInput::Buffer(buf) => {
            read_metadata(&mut Cursor::new(buf)).map_err(RbPolarsErr::from)?
        }
        RubyScanSourceInput::Path(p) => {
            let cloud_options = parse_cloud_options(
                Some(p.as_ref()),
                storage_options,
                credential_provider,
                retries,
            )?;
            match p {
                PlPath::Local(local) => {
                    let file = polars_utils::open_file(&local).map_err(RbPolarsErr::from)?;
                    read_metadata(&mut BufReader::new(file)).map_err(RbPolarsErr::from)?
                }
                PlPath::Cloud(_) => {
                    use polars::prelude::ParquetObjectStore;
                    use polars_error::PolarsResult;

                    rb.detach(|| {
                        get_runtime().block_on(async {
                            let mut reader = ParquetObjectStore::from_uri(
                                p.as_ref(),
                                cloud_options.as_ref(),
                                None,
                            )
                            .await?;
                            let result = reader.get_metadata().await?;
                            PolarsResult::Ok((**result).clone())
                        })
                    })
                    .map_err(RbPolarsErr::from)?
                }
            }
        }
        RubyScanSourceInput::File(f) => {
            read_metadata(&mut BufReader::new(f)).map_err(RbPolarsErr::from)?
        }
    };

    let key_value_metadata = read_custom_key_value_metadata(metadata.key_value_metadata());
    let dict = rb.hash_new();
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

pub fn parse_cloud_options<'a>(
    first_path: Option<PlPathRef<'a>>,
    storage_options: Option<Vec<(String, String)>>,
    credential_provider: Option<Value>,
    retries: usize,
) -> RbResult<Option<CloudOptions>> {
    let result = if let Some(first_path) = first_path {
        use crate::prelude::parse_cloud_options;

        let first_path_url = first_path.to_str();
        let cloud_options =
            parse_cloud_options(first_path_url, storage_options.unwrap_or_default())?;

        Some(
            cloud_options
                .with_max_retries(retries)
                .with_credential_provider(credential_provider.map(|_| todo!())),
        )
    } else {
        None
    };
    Ok(result)
}

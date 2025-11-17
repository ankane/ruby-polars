use magnus::prelude::*;
use magnus::{RHash, Value};
use polars::prelude::{ArrowDataType, DataType};
use polars_error::polars_err;

use crate::interop::arrow::to_rust::normalize_arrow_fields;
use crate::prelude::Wrap;
use crate::series::import_schema_rbcapsule;
use crate::utils::to_rb_err;
use crate::{RbResult, RbValueError};

pub mod to_ruby;
pub mod to_rust;

pub fn init_polars_schema_from_arrow_c_schema(
    polars_schema: RHash,
    schema_object: Value,
) -> RbResult<()> {
    let schema_capsule = schema_object.funcall("arrow_c_schema", ())?;

    let field = import_schema_rbcapsule(schema_capsule)?;
    let field = normalize_arrow_fields(&field);

    let ArrowDataType::Struct(fields) = field.dtype else {
        return Err(RbValueError::new_err(format!(
            "arrow_c_schema of object passed to Polars::Schema did not return struct dtype: \
            object: {}, dtype: {:?}",
            schema_object, &field.dtype
        )));
    };

    for field in fields {
        let dtype = DataType::from_arrow_field(&field);

        let name = field.name.as_str();
        let dtype = Wrap(dtype);

        if polars_schema.get(name).is_some() {
            return Err(to_rb_err(polars_err!(
                Duplicate:
                "arrow schema contained duplicate name: {}",
                name
            )));
        }

        polars_schema.aset(name, dtype)?;
    }

    Ok(())
}

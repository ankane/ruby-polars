use std::any::Any;
use std::sync::Arc;

use magnus::{value::Opaque, IntoValue, Ruby, Value};
use polars::error::PolarsError::ComputeError;
use polars::prelude::*;
use polars_core::chunked_array::object::builder::ObjectChunkedBuilder;
use polars_core::chunked_array::object::registry;
use polars_core::chunked_array::object::registry::AnonymousObjectBuilder;
use polars_core::prelude::AnyValue;

use crate::map::lazy::call_lambda_with_series;
use crate::map::lazy::to_series;
use crate::map::ruby_udf;
use crate::prelude::ObjectValue;
use crate::Wrap;

fn ruby_function_caller_series(s: Series, lambda: Opaque<Value>) -> PolarsResult<Series> {
    let lambda = Ruby::get().unwrap().get_inner(lambda);
    let object = call_lambda_with_series(s.clone(), lambda)
        .map_err(|s| ComputeError(format!("{}", s).into()))?;
    to_series(object, s.name())
}

pub(crate) fn register_startup_deps() {
    if !registry::is_object_builder_registered() {
        let object_builder = Box::new(|name: PlSmallStr, capacity: usize| {
            Box::new(ObjectChunkedBuilder::<ObjectValue>::new(name, capacity))
                as Box<dyn AnonymousObjectBuilder>
        });

        let object_converter = Arc::new(|av: AnyValue| {
            let object = ObjectValue {
                inner: Wrap(av).into_value().into(),
            };
            Box::new(object) as Box<dyn Any>
        });

        let object_size = std::mem::size_of::<ObjectValue>();
        let physical_dtype = ArrowDataType::FixedSizeBinary(object_size);
        registry::register_object_builder(object_builder, object_converter, physical_dtype);

        unsafe { ruby_udf::CALL_SERIES_UDF_RUBY = Some(ruby_function_caller_series) }
    }
}

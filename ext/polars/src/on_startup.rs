use std::any::Any;
use std::sync::Arc;

use magnus::IntoValue;
use polars::prelude::*;
use polars_core::chunked_array::object::builder::ObjectChunkedBuilder;
use polars_core::chunked_array::object::registry;
use polars_core::chunked_array::object::registry::AnonymousObjectBuilder;
use polars_core::prelude::AnyValue;

use crate::prelude::ObjectValue;
use crate::Wrap;

pub(crate) fn register_object_builder() {
    if !registry::is_object_builder_registered() {
        let object_builder = Box::new(|name: &str, capacity: usize| {
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
        registry::register_object_builder(object_builder, object_converter, physical_dtype)
    }
}

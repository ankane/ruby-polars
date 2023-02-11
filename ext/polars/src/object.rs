use std::any::Any;
use std::sync::Arc;

use magnus::IntoValue;
use polars_core::chunked_array::object::builder::ObjectChunkedBuilder;
use polars_core::chunked_array::object::registry;
use polars_core::chunked_array::object::registry::AnonymousObjectBuilder;
use polars_core::prelude::AnyValue;

use crate::prelude::ObjectValue;
use crate::Wrap;

// pub(crate) const OBJECT_NAME: &str = "object";

pub(crate) fn register_object_builder() {
    if !registry::is_object_builder_registered() {
        let object_builder = Box::new(|name: &str, capacity: usize| {
            Box::new(ObjectChunkedBuilder::<ObjectValue>::new(name, capacity))
                as Box<dyn AnonymousObjectBuilder>
        });

        let object_converter = Arc::new(|av: AnyValue| {
            let object = ObjectValue {
                inner: Wrap(av).into_value(),
            };
            Box::new(object) as Box<dyn Any>
        });

        registry::register_object_builder(object_builder, object_converter)
    }
}

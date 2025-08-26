use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;

use magnus::{IntoValue, Ruby};
use polars::prelude::*;
use polars_core::chunked_array::object::builder::ObjectChunkedBuilder;
use polars_core::chunked_array::object::registry;
use polars_core::chunked_array::object::registry::AnonymousObjectBuilder;
use polars_core::prelude::AnyValue;

use crate::Wrap;
use crate::prelude::ObjectValue;

static POLARS_REGISTRY_INIT_LOCK: OnceLock<()> = OnceLock::new();

pub(crate) fn register_startup_deps() {
    POLARS_REGISTRY_INIT_LOCK.get_or_init(|| {
        let object_builder = Box::new(|name: PlSmallStr, capacity: usize| {
            Box::new(ObjectChunkedBuilder::<ObjectValue>::new(name, capacity))
                as Box<dyn AnonymousObjectBuilder>
        });

        let object_converter = Arc::new(|av: AnyValue| {
            let object = ObjectValue {
                inner: Wrap(av).into_value_with(&Ruby::get().unwrap()).into(),
            };
            Box::new(object) as Box<dyn Any>
        });
        let rbobject_converter = Arc::new(|av: AnyValue| {
            let object = Wrap(av).into_value_with(&Ruby::get().unwrap());
            Box::new(object) as Box<dyn Any>
        });

        let object_size = std::mem::size_of::<ObjectValue>();
        let physical_dtype = ArrowDataType::FixedSizeBinary(object_size);
        registry::register_object_builder(
            object_builder,
            object_converter,
            rbobject_converter,
            physical_dtype,
        );
        // TODO
        // Register warning function for `polars_warn!`.
        // polars_error::set_warning_function(warning_function);
    });
}

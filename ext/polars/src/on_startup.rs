#![allow(unsafe_op_in_unsafe_fn)]
use std::any::Any;
use std::sync::Arc;
use std::sync::OnceLock;

use arrow::array::Array;
use magnus::{IntoValue, Ruby, Value, prelude::*, value::Opaque};
use polars::prelude::*;
use polars_core::chunked_array::object::builder::ObjectChunkedBuilder;
use polars_core::chunked_array::object::registry;
use polars_core::chunked_array::object::registry::AnonymousObjectBuilder;
use polars_core::prelude::AnyValue;
use polars_error::PolarsWarning;
use polars_error::signals::register_polars_keyboard_interrupt_hook;

use crate::Wrap;
use crate::map::lazy::call_lambda_with_series;
use crate::map::ruby_udf;
use crate::prelude::ObjectValue;
use crate::rb_modules::pl_utils;
use crate::utils::RubyAttach;

fn ruby_function_caller_series(
    s: &[Column],
    output_dtype: Option<DataType>,
    lambda: Opaque<Value>,
) -> PolarsResult<Column> {
    Ruby::attach(|rb| call_lambda_with_series(rb, s, output_dtype, lambda))
}

fn warning_function(msg: &str, _warning: PolarsWarning) {
    Ruby::attach(|rb| {
        if let Err(e) = pl_utils(rb).funcall::<_, _, Value>("_polars_warn", (msg.to_string(),)) {
            eprintln!("{e}")
        }
    })
}

static POLARS_REGISTRY_INIT_LOCK: OnceLock<()> = OnceLock::new();

pub unsafe fn register_startup_deps(catch_keyboard_interrupt: bool) {
    POLARS_REGISTRY_INIT_LOCK.get_or_init(|| {
        let object_builder = Box::new(|name: PlSmallStr, capacity: usize| {
            Box::new(ObjectChunkedBuilder::<ObjectValue>::new(name, capacity))
                as Box<dyn AnonymousObjectBuilder>
        });

        let object_converter = Arc::new(|av: AnyValue| {
            let object = Ruby::attach(|rb| ObjectValue {
                inner: Wrap(av).into_value_with(rb).into(),
            });
            Box::new(object) as Box<dyn Any>
        });
        let rbobject_converter = Arc::new(|av: AnyValue| {
            let object = Ruby::attach(|rb| Wrap(av).into_value_with(rb));
            Box::new(object) as Box<dyn Any>
        });
        fn object_array_getter(_arr: &dyn Array, _idx: usize) -> Option<AnyValue<'_>> {
            todo!();
        }

        let object_size = std::mem::size_of::<ObjectValue>();
        let physical_dtype = ArrowDataType::FixedSizeBinary(object_size);
        registry::register_object_builder(
            object_builder,
            object_converter,
            rbobject_converter,
            physical_dtype,
            Arc::new(object_array_getter),
        );

        // Register SERIES UDF.
        ruby_udf::CALL_COLUMNS_UDF_RUBY = Some(ruby_function_caller_series);
        // Register warning function for `polars_warn!`.
        polars_error::set_warning_function(warning_function);

        if catch_keyboard_interrupt {
            register_polars_keyboard_interrupt_hook();
        }
    });
}

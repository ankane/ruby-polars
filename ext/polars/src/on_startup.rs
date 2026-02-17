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
use crate::dataframe::RbDataFrame;
use crate::lazyframe::RbLazyFrame;
use crate::map::lazy::call_lambda_with_series;
use crate::prelude::ObjectValue;
use crate::rb_modules::pl_utils;
use crate::ruby::gvl::GvlExt;
use crate::ruby::ruby_convert_registry::{FromRubyConvertRegistry, RubyConvertRegistry};
use crate::ruby::ruby_udf;
use crate::ruby::thread::{is_non_ruby_thread, run_in_ruby_thread};
use crate::ruby::utils::to_pl_err;
use crate::series::RbSeries;

fn ruby_function_caller_series(
    s: &[Column],
    output_dtype: Option<DataType>,
    lambda: Opaque<Value>,
) -> PolarsResult<Column> {
    if is_non_ruby_thread() {
        let s2 = s.to_vec();
        return run_in_ruby_thread(move || ruby_function_caller_series(&s2, output_dtype, lambda));
    }

    Ruby::attach(|rb| call_lambda_with_series(rb, s, output_dtype, lambda))
}

fn ruby_function_caller_df(df: DataFrame, lambda: Opaque<Value>) -> PolarsResult<DataFrame> {
    Ruby::attach(|rb| {
        let lambda = rb.get_inner(lambda);

        let rbpolars = pl_utils(rb);

        // create a RbSeries struct/object for Ruby
        let rbdf = RbDataFrame::new(df);
        // Wrap this RbSeries object in the Ruby side Series wrapper
        let ruby_df_wrapper: Value = rbpolars.funcall("wrap_df", (rbdf,)).map_err(to_pl_err)?;

        // call the lambda and get a Ruby side df wrapper
        let result_df_wrapper: Value = lambda
            .funcall("call", (ruby_df_wrapper,))
            .map_err(to_pl_err)?;

        // unpack the wrapper in a RbDataFrame
        let rbdf: &RbDataFrame = result_df_wrapper.funcall("_df", ()).map_err(|_| {
            let rbtype = unsafe { result_df_wrapper.classname() };
            PolarsError::ComputeError(
                format!("Expected 'LazyFrame.map' to return a 'DataFrame', got a '{rbtype}'",)
                    .into(),
            )
        })?;
        Ok(rbdf.clone().df.into_inner())
    })
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

        crate::ruby::ruby_convert_registry::register_converters(RubyConvertRegistry {
            from_rb: FromRubyConvertRegistry {
                file_provider_result: Arc::new(|_rb_f| Ruby::attach(|_rb| todo!())),
                series: Arc::new(|rb_f| {
                    Ruby::attach(|rb| {
                        Ok(Box::new(
                            <&RbSeries>::try_convert(rb.get_inner(rb_f))?
                                .clone()
                                .series
                                .into_inner(),
                        ) as _)
                    })
                }),
                df: Arc::new(|rb_f| {
                    Ruby::attach(|rb| {
                        Ok(Box::new(
                            <&RbDataFrame>::try_convert(rb.get_inner(rb_f))?
                                .clone()
                                .df
                                .into_inner(),
                        ) as _)
                    })
                }),
                dsl_plan: Arc::new(|rb_f| {
                    Ruby::attach(|rb| {
                        Ok(Box::new(
                            <&RbLazyFrame>::try_convert(rb.get_inner(rb_f))?
                                .clone()
                                .ldf
                                .into_inner()
                                .logical_plan,
                        ) as _)
                    })
                }),
                schema: Arc::new(|rb_f| {
                    Ruby::attach(|rb| {
                        Ok(Box::new(
                            Wrap::<polars_core::schema::Schema>::try_convert(rb.get_inner(rb_f))?.0,
                        ) as _)
                    })
                }),
            },
            to_rb: crate::ruby::ruby_convert_registry::ToRubyConvertRegistry {
                df: Arc::new(|df| {
                    Ruby::attach(|rb| {
                        Ok(
                            RbDataFrame::new(df.downcast_ref::<DataFrame>().unwrap().clone())
                                .into_value_with(rb),
                        )
                    })
                }),
                series: Arc::new(|series| {
                    Ruby::attach(|rb| {
                        Ok(
                            RbSeries::new(series.downcast_ref::<Series>().unwrap().clone())
                                .into_value_with(rb),
                        )
                    })
                }),
                dsl_plan: Arc::new(|dsl_plan| {
                    Ruby::attach(|rb| {
                        Ok(RbLazyFrame::from(LazyFrame::from(
                            dsl_plan
                                .downcast_ref::<polars_plan::dsl::DslPlan>()
                                .unwrap()
                                .clone(),
                        ))
                        .into_value_with(rb))
                    })
                }),
                schema: Arc::new(|_schema| Ruby::attach(|_rb| todo!())),
            },
        });

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
        // Register DATAFRAME UDF.
        ruby_udf::CALL_DF_UDF_RUBY = Some(ruby_function_caller_df);
        // Register warning function for `polars_warn!`.
        polars_error::set_warning_function(warning_function);

        if catch_keyboard_interrupt {
            register_polars_keyboard_interrupt_hook();
        }
    });
}

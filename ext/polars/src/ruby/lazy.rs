use magnus::{Ruby, Value, value::Opaque};
use polars::prelude::{AllowedOptimizations, DataFrame, LazyFrame, SchemaRef};

use crate::ruby::ruby_function::RubyFunction;
use crate::ruby::ruby_udf::CALL_DF_UDF_RUBY;
use crate::ruby::thread::{is_non_ruby_thread, run_in_ruby_thread, start_background_ruby_thread};
use crate::ruby::utils::ArcValue;

pub trait RubyUdfLazyFrameExt {
    fn map_ruby(
        self,
        function: RubyFunction,
        optimizations: AllowedOptimizations,
        schema: Option<SchemaRef>,
        validate_output: bool,
    ) -> LazyFrame;
}

impl RubyUdfLazyFrameExt for LazyFrame {
    fn map_ruby(
        self,
        function: RubyFunction,
        optimizations: AllowedOptimizations,
        _schema: Option<SchemaRef>,
        _validate_output: bool,
    ) -> LazyFrame {
        let f = move |df: DataFrame, lambda: Opaque<Value>| {
            let func = unsafe { CALL_DF_UDF_RUBY.unwrap() };
            func(df, lambda)
        };

        // handle non-Ruby threads
        start_background_ruby_thread(&Ruby::get_with(function.0));
        let udf = ArcValue::new(function.0);
        let f = move |df| {
            if is_non_ruby_thread() {
                let udf = udf.clone();
                return run_in_ruby_thread(move |_rb| f(df, *udf.0));
            }
            f(df, *udf.0)
        };

        let schema = None;
        self.map(f, optimizations, schema, Some("RUBY UDF"))
    }
}

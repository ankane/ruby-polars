use magnus::Ruby;
use polars::prelude::{AllowedOptimizations, DataFrame, LazyFrame, SchemaRef};

use crate::ruby::ruby_function::RubyFunction;
use crate::ruby::ruby_udf::CALL_DF_UDF_RUBY;
use crate::ruby::thread::{is_non_ruby_thread, run_in_ruby_thread, start_background_ruby_thread};
use crate::ruby::utils::BoxOpaque;

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
        let boxed = BoxOpaque::new(function.0);

        start_background_ruby_thread(&Ruby::get().unwrap());

        let function = move |df: DataFrame, boxed: &BoxOpaque| {
            let func = unsafe { CALL_DF_UDF_RUBY.unwrap() };
            func(df, *boxed.0)
        };
        let function = move |df| {
            if is_non_ruby_thread() {
                let boxed = boxed.clone();
                return run_in_ruby_thread(move |_rb| function(df, &boxed));
            }

            function(df, &boxed)
        };
        let schema = None;
        self.map(function, optimizations, schema, Some("RUBY UDF"))
    }
}

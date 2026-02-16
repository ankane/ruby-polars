use polars::prelude::{AllowedOptimizations, LazyFrame, SchemaRef};

use crate::ruby::ruby_function::RubyFunction;
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
        let _boxed = BoxOpaque::new(function.0);

        let function = move |_| todo!();
        let schema = None;
        self.map(function, optimizations, schema, Some("RUBY UDF"))
    }
}

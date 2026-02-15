use std::sync::{Arc, OnceLock};

use magnus::{Value, value::Opaque};
use polars_core::datatypes::Field;
use polars_core::error::*;
use polars_core::frame::column::Column;
use polars_core::schema::Schema;
use polars_plan::prelude::*;
use polars_utils::pl_str::PlSmallStr;

#[allow(unused)]
pub struct RubyUdfExpression {
    ruby_function: Opaque<Value>,
    output_type: Option<DataTypeExpr>,
    materialized_field: OnceLock<Field>,
    is_elementwise: bool,
    returns_scalar: bool,
}

impl RubyUdfExpression {
    pub fn new(
        lambda: Value,
        output_type: Option<impl Into<DataTypeExpr>>,
        is_elementwise: bool,
        returns_scalar: bool,
    ) -> Self {
        let output_type = output_type.map(Into::into);
        Self {
            ruby_function: lambda.into(),
            output_type,
            materialized_field: OnceLock::new(),
            is_elementwise,
            returns_scalar,
        }
    }
}

impl ColumnsUdf for RubyUdfExpression {
    #[allow(unused)]
    fn call_udf(&self, s: &mut [Column]) -> PolarsResult<Column> {
        todo!();
    }
}

impl AnonymousColumnsUdf for RubyUdfExpression {
    #[allow(unused)]
    fn as_column_udf(self: Arc<Self>) -> Arc<dyn ColumnsUdf> {
        self as _
    }

    #[allow(unused)]
    fn deep_clone(self: Arc<Self>) -> Arc<dyn AnonymousColumnsUdf> {
        todo!();
    }

    #[allow(unused)]
    fn try_serialize(&self, buf: &mut Vec<u8>) -> PolarsResult<()> {
        todo!();
    }

    #[allow(unused)]
    fn get_field(&self, input_schema: &Schema, fields: &[Field]) -> PolarsResult<Field> {
        todo!();
    }
}

pub fn map_many_ruby(exprs: Vec<Expr>, func: RubyUdfExpression) -> Expr {
    const NAME: &str = "ruby_udf";

    let returns_scalar = func.returns_scalar;

    let mut flags = FunctionFlags::default() | FunctionFlags::OPTIONAL_RE_ENTRANT;
    if func.is_elementwise {
        flags.set_elementwise();
    }
    if returns_scalar {
        flags |= FunctionFlags::RETURNS_SCALAR;
    }

    Expr::AnonymousFunction {
        input: exprs,
        function: new_column_udf(func),
        options: FunctionOptions {
            flags,
            ..Default::default()
        },
        fmt_str: Box::new(PlSmallStr::from(NAME)),
    }
}

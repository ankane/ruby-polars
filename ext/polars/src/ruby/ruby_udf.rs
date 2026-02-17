use std::sync::{Arc, OnceLock};

use magnus::{Ruby, Value, value::Opaque, value::ReprValue};
use polars::prelude::*;
use polars_core::datatypes::{DataType, Field};
use polars_core::frame::column::Column;
use polars_core::schema::Schema;
use polars_plan::dsl::udf::try_infer_udf_output_dtype;
use polars_plan::prelude::*;
use polars_utils::pl_str::PlSmallStr;

use crate::ruby::gvl::GvlExt;
use crate::ruby::utils::ArcValue;

#[allow(clippy::type_complexity)]
pub static mut CALL_COLUMNS_UDF_RUBY: Option<
    fn(s: &[Column], output_dtype: Option<DataType>, lambda: Opaque<Value>) -> PolarsResult<Column>,
> = None;

#[allow(clippy::type_complexity)]
pub static mut CALL_DF_UDF_RUBY: Option<
    fn(s: DataFrame, lambda: Opaque<Value>) -> PolarsResult<DataFrame>,
> = None;

pub struct RubyUdfExpression {
    ruby_function: ArcValue,
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
            ruby_function: ArcValue::new(lambda),
            output_type,
            materialized_field: OnceLock::new(),
            is_elementwise,
            returns_scalar,
        }
    }
}

impl ColumnsUdf for RubyUdfExpression {
    fn call_udf(&self, s: &mut [Column]) -> PolarsResult<Column> {
        let func = unsafe { CALL_COLUMNS_UDF_RUBY.unwrap() };
        let field = self
            .materialized_field
            .get()
            .expect("should have been materialized at this point");
        let mut out = func(
            s,
            self.materialized_field.get().map(|f| f.dtype.clone()),
            *self.ruby_function.0,
        )?;

        let must_cast = out.dtype().matches_schema_type(field.dtype()).map_err(|_| {
            polars_err!(
                SchemaMismatch: "expected output type '{:?}', got '{:?}'; set `return_dtype` to the proper datatype",
                field.dtype(), out.dtype(),
            )
        })?;
        if must_cast {
            out = out.cast(field.dtype())?;
        }

        Ok(out)
    }
}

impl AnonymousColumnsUdf for RubyUdfExpression {
    fn as_column_udf(self: Arc<Self>) -> Arc<dyn ColumnsUdf> {
        self as _
    }

    fn deep_clone(self: Arc<Self>) -> Arc<dyn AnonymousColumnsUdf> {
        Arc::new(Self {
            ruby_function: ArcValue::new(
                Ruby::attach(|rb| {
                    // TODO fix
                    rb.get_inner(*self.ruby_function.0)
                        .funcall::<_, _, Value>("dup", ())
                })
                .unwrap(),
            ),
            output_type: self.output_type.clone(),
            materialized_field: OnceLock::new(),
            is_elementwise: self.is_elementwise,
            returns_scalar: self.returns_scalar,
        }) as _
    }

    fn try_serialize(&self, _buf: &mut Vec<u8>) -> PolarsResult<()> {
        todo!();
    }

    fn get_field(&self, input_schema: &Schema, fields: &[Field]) -> PolarsResult<Field> {
        let field = match self.materialized_field.get() {
            Some(f) => f.clone(),
            None => {
                let dtype = match self.output_type.as_ref() {
                    None => {
                        let func = unsafe { CALL_COLUMNS_UDF_RUBY.unwrap() };
                        let f = |s: &[Column]| func(s, None, *self.ruby_function.0);
                        try_infer_udf_output_dtype(&f as _, fields)?
                    }
                    Some(output_type) => output_type
                        .clone()
                        .into_datatype_with_self(input_schema, fields[0].dtype())?,
                };

                // Take the name of first field, just like `map_field`.
                let name = fields[0].name();
                let f = Field::new(name.clone(), dtype);
                self.materialized_field.get_or_init(|| f.clone());
                f
            }
        };
        Ok(field)
    }
}

pub trait RubyUdfExt {
    #[allow(unused)]
    fn map_ruby(self, func: RubyUdfExpression) -> Expr;

    fn map_many_ruby(exprs: Vec<Expr>, func: RubyUdfExpression) -> Expr;
}

impl RubyUdfExt for Expr {
    fn map_ruby(self, func: RubyUdfExpression) -> Expr {
        Self::map_many_ruby(vec![self], func)
    }

    fn map_many_ruby(exprs: Vec<Expr>, func: RubyUdfExpression) -> Expr {
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
}

use magnus::{value::Opaque, Value};
use polars::prelude::*;
use polars_plan::prelude::{ApplyOptions, Context, FunctionFlags, FunctionOptions};

// Will be overwritten on Ruby Polars start up.
pub static mut CALL_SERIES_UDF_RUBY: Option<
    fn(s: Series, lambda: Opaque<Value>) -> PolarsResult<Series>,
> = None;

pub struct RubyUdfExpression {
    ruby_function: Opaque<Value>,
    output_type: Option<DataType>,
    is_elementwise: bool,
    returns_scalar: bool,
}

impl RubyUdfExpression {
    pub fn new(
        lambda: Opaque<Value>,
        output_type: Option<DataType>,
        is_elementwise: bool,
        returns_scalar: bool,
    ) -> Self {
        Self {
            ruby_function: lambda,
            output_type,
            is_elementwise,
            returns_scalar,
        }
    }
}

impl SeriesUdf for RubyUdfExpression {
    fn call_udf(&self, s: &mut [Series]) -> PolarsResult<Option<Series>> {
        let func = unsafe { CALL_SERIES_UDF_RUBY.unwrap() };

        let output_type = self
            .output_type
            .clone()
            .unwrap_or_else(|| DataType::Unknown(Default::default()));
        let mut out = func(s[0].clone(), self.ruby_function)?;
        if !matches!(output_type, DataType::Unknown(_)) {
            let must_cast = out.dtype().matches_schema_type(&output_type).map_err(|_| {
                polars_err!(
                    SchemaMismatch: "expected output type '{:?}', got '{:?}'; set `return_dtype` to the proper datatype",
                    output_type, out.dtype(),
                )
            })?;
            if must_cast {
                out = out.cast(&output_type)?;
            }
        }

        Ok(Some(out))
    }
}

pub struct RubyGetOutput {
    return_dtype: Option<DataType>,
}

impl RubyGetOutput {
    pub fn new(return_dtype: Option<DataType>) -> Self {
        Self { return_dtype }
    }
}

impl FunctionOutputField for RubyGetOutput {
    fn get_field(
        &self,
        _input_schema: &Schema,
        _cntxt: Context,
        fields: &[Field],
    ) -> PolarsResult<Field> {
        // Take the name of first field, just like [`GetOutput::map_field`].
        let name = fields[0].name();
        let return_dtype = match self.return_dtype {
            Some(ref dtype) => dtype.clone(),
            None => DataType::Unknown(Default::default()),
        };
        Ok(Field::new(name.clone(), return_dtype))
    }
}

pub fn map_ruby(expr: Expr, func: RubyUdfExpression, agg_list: bool) -> Expr {
    let (collect_groups, name) = if agg_list {
        (ApplyOptions::ApplyList, "map_list")
    } else if func.is_elementwise {
        (ApplyOptions::ElementWise, "ruby_udf")
    } else {
        (ApplyOptions::GroupWise, "ruby_udf")
    };

    let returns_scalar = func.returns_scalar;
    let return_dtype = func.output_type.clone();

    let output_field = RubyGetOutput::new(return_dtype);
    let output_type = SpecialEq::new(Arc::new(output_field) as Arc<dyn FunctionOutputField>);

    let mut flags = FunctionFlags::default() | FunctionFlags::OPTIONAL_RE_ENTRANT;
    if returns_scalar {
        flags |= FunctionFlags::RETURNS_SCALAR;
    }

    Expr::AnonymousFunction {
        input: vec![expr],
        function: SpecialEq::new(Arc::new(func)),
        output_type,
        options: FunctionOptions {
            collect_groups,
            fmt_str: name,
            flags,
            ..Default::default()
        },
    }
}

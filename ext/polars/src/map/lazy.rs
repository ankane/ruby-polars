use magnus::{KwArgs, RArray, Ruby, Value, prelude::*, value::Opaque};
use polars::prelude::*;

use crate::expr::ToExprs;
use crate::expr::datatype::RbDataTypeExpr;
use crate::ruby::ruby_udf::{RubyUdfExpression, RubyUdfExt};
use crate::ruby::utils::to_pl_err;
use crate::{RbExpr, RbResult, RbSeries, Wrap};

pub(crate) fn call_lambda_with_series(
    rb: &Ruby,
    s: &[Column],
    output_dtype: Option<DataType>,
    lambda: Opaque<Value>,
) -> PolarsResult<Column> {
    let lambda = rb.get_inner(lambda);

    // Set return_dtype in kwargs
    let dict = rb.hash_new();
    let output_dtype = output_dtype.map(Wrap);
    dict.aset(rb.sym_new("return_dtype"), output_dtype)
        .map_err(to_pl_err)?;

    let series_objects = rb.ary_from_iter(
        s.iter()
            .map(|c| RbSeries::new(c.as_materialized_series().clone())),
    );

    let result = lambda.funcall::<_, _, &RbSeries>("call", (series_objects, KwArgs(dict)));
    result
        .map_err(to_pl_err)
        .map(|s| s.clone().series.into_inner().into_column())
}

pub fn map_expr(
    rbexpr: RArray,
    lambda: Value,
    output_type: Option<&RbDataTypeExpr>,
    is_elementwise: bool,
    returns_scalar: bool,
) -> RbResult<RbExpr> {
    let output_type = output_type.map(|v| v.inner.clone());
    let func = RubyUdfExpression::new(lambda, output_type, is_elementwise, returns_scalar);
    let exprs = rbexpr.to_exprs()?;
    Ok(Expr::map_many_ruby(exprs, func).into())
}

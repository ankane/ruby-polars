use magnus::{RArray, Ruby, Value, prelude::*, value::Opaque};
use polars::prelude::*;

use crate::expr::ToExprs;
use crate::expr::datatype::RbDataTypeExpr;
use crate::map::ruby_udf::{RubyUdfExpression, map_many_ruby};
use crate::rb_modules::*;
use crate::{RbExpr, RbResult, RbSeries, Wrap};

fn to_series(v: Value, name: &str) -> PolarsResult<Series> {
    let ruby = Ruby::get_with(v);
    let rb_rbseries = match v.funcall("_s", ()) {
        Ok(s) => s,
        // the lambda did not return a series, we try to create a new Ruby Series
        _ => {
            let data = ruby.ary_new();
            data.push(v).unwrap();
            let res = pl_series(&ruby).funcall::<_, _, Value>("new", (name.to_string(), data));

            match res {
                Ok(ruby_s) => ruby_s.funcall::<_, _, &RbSeries>("_s", ()).unwrap(),
                Err(_) => {
                    polars_bail!(ComputeError:
                        "expected a something that could convert to a `Series` but got: {}",
                        unsafe { v.classname() }
                    )
                }
            }
        }
    };
    // Finally get the actual Series
    Ok(rb_rbseries.series.read().clone())
}

pub fn binary_lambda(lambda: Value, a: Series, b: Series) -> PolarsResult<Option<Series>> {
    let ruby = Ruby::get_with(lambda);

    // create a RbSeries struct/object for Ruby
    let rbseries_a = RbSeries::new(a);
    let rbseries_b = RbSeries::new(b);

    // Wrap this RbSeries object in the Ruby side Series wrapper
    let ruby_series_wrapper_a: Value = pl_utils(&ruby).funcall("wrap_s", (rbseries_a,)).unwrap();
    let ruby_series_wrapper_b: Value = pl_utils(&ruby).funcall("wrap_s", (rbseries_b,)).unwrap();

    // call the lambda and get a Ruby side Series wrapper
    let result_series_wrapper: Value =
        match lambda.funcall("call", (ruby_series_wrapper_a, ruby_series_wrapper_b)) {
            Ok(rbobj) => rbobj,
            Err(e) => polars_bail!(
                ComputeError: "custom Ruby function failed: {}", e,
            ),
        };
    let rbseries = if let Ok(rbexpr) = result_series_wrapper.funcall::<_, _, &RbExpr>("_rbexpr", ())
    {
        let expr = rbexpr.inner.clone();
        let df = DataFrame::empty();
        let out = df
            .lazy()
            .select([expr])
            .with_predicate_pushdown(false)
            .with_projection_pushdown(false)
            .collect()?;

        let s = out.select_at_idx(0).unwrap().clone();
        RbSeries::new(s.take_materialized_series())
    } else {
        return Some(to_series(result_series_wrapper, "")).transpose();
    };

    // Finally get the actual Series
    let binding = rbseries.series.read();
    Ok(Some(binding.clone()))
}

#[allow(unused_variables)]
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
    dict.aset("return_dtype", output_dtype).unwrap();

    let series_objects = rb.ary_from_iter(
        s.iter()
            .map(|c| RbSeries::new(c.as_materialized_series().clone())),
    );

    let result = lambda.funcall::<_, _, &RbSeries>("call", (series_objects, dict));
    Ok(result
        .map(|s| s.clone().series.into_inner().into_column())
        .unwrap())
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
    Ok(map_many_ruby(exprs, func).into())
}

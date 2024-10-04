use magnus::{prelude::*, value::Opaque, RArray, Value};
use polars::prelude::*;

use super::ruby_udf;
use crate::rb_modules::*;
use crate::{RbExpr, RbResult, RbSeries, Wrap};

pub fn to_series(v: Value, name: &str) -> PolarsResult<Series> {
    let rb_rbseries = match v.funcall("_s", ()) {
        Ok(s) => s,
        // the lambda did not return a series, we try to create a new Ruby Series
        _ => {
            let data = RArray::new();
            data.push(v).unwrap();
            let res = series().funcall::<_, _, Value>("new", (name.to_string(), data));

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
    Ok(rb_rbseries.series.borrow().clone())
}

pub(crate) fn call_lambda_with_series(s: Series, lambda: Value) -> RbResult<Value> {
    // create a RbSeries struct/object for Ruby
    let rbseries = RbSeries::new(s);
    // Wrap this RbSeries object in the Ruby side Series wrapper
    let ruby_series_wrapper: Value = utils().funcall("wrap_s", (rbseries,)).unwrap();
    // call the lambda and get a Ruby side Series wrapper
    lambda.funcall("call", (ruby_series_wrapper,))
}

pub fn binary_lambda(lambda: Value, a: Series, b: Series) -> PolarsResult<Option<Series>> {
    // create a RbSeries struct/object for Ruby
    let rbseries_a = RbSeries::new(a);
    let rbseries_b = RbSeries::new(b);

    // Wrap this RbSeries object in the Ruby side Series wrapper
    let ruby_series_wrapper_a: Value = utils().funcall("wrap_s", (rbseries_a,)).unwrap();
    let ruby_series_wrapper_b: Value = utils().funcall("wrap_s", (rbseries_b,)).unwrap();

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
        RbSeries::new(s)
    } else {
        return Some(to_series(result_series_wrapper, "")).transpose();
    };

    // Finally get the actual Series
    let binding = rbseries.series.borrow();
    Ok(Some(binding.clone()))
}

pub fn map_single(
    rbexpr: &RbExpr,
    lambda: Value,
    output_type: Option<Wrap<DataType>>,
    agg_list: bool,
    is_elementwise: bool,
    returns_scalar: bool,
) -> RbExpr {
    let output_type = output_type.map(|wrap| wrap.0);

    let func = ruby_udf::RubyUdfExpression::new(
        Opaque::from(lambda),
        output_type,
        is_elementwise,
        returns_scalar,
    );
    ruby_udf::map_ruby(rbexpr.inner.clone(), func, agg_list).into()
}

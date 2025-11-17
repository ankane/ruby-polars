use magnus::{Ruby, Value, prelude::*};
use polars::prelude::*;

use crate::rb_modules::*;
use crate::{RbExpr, RbSeries, Wrap};

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
    let ruby = Ruby::get().unwrap();

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

pub fn map_single(
    _rbexpr: &RbExpr,
    _lambda: Value,
    _output_type: Option<Wrap<DataType>>,
    _agg_list: bool,
    _is_elementwise: bool,
    _returns_scalar: bool,
) -> RbExpr {
    todo!();
}

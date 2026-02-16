use magnus::{IntoValue, Ruby, Value, value::Opaque, value::ReprValue};

use super::RbSeries;
use crate::map::series::ApplyLambdaGeneric;
use crate::prelude::*;
use crate::ruby::gvl::RubyAttach;
use crate::series::construction::series_from_objects;
use crate::{RbPolarsErr, RbResult};
use crate::{apply_all_polars_dtypes, raise_err};

impl RbSeries {
    pub fn map_elements(
        &self,
        function: Value,
        return_dtype: Option<Wrap<DataType>>,
        skip_nulls: bool,
    ) -> RbResult<Self> {
        let series = &self.series.read().clone(); // Clone so we don't deadlock on re-entrance.

        if return_dtype.is_none() {
            polars_warn!(
                MapWithoutReturnDtypeWarning,
                "Calling `map_elements` without specifying `return_dtype` can lead to unpredictable results. \
                Specify `return_dtype` to silence this warning."
            )
        }

        if skip_nulls && (series.null_count() == series.len()) {
            if let Some(return_dtype) = return_dtype {
                return Ok(
                    Series::full_null(series.name().clone(), series.len(), &return_dtype.0).into(),
                );
            }
            let msg = "The output type of the 'map_elements' function cannot be determined.\n\
            The function was never called because 'skip_nulls: true' and all values are null.\n\
            Consider setting 'skip_nulls: false' or setting the 'return_dtype'.";
            raise_err!(msg, ComputeError)
        }

        let return_dtype = return_dtype.map(|dt| dt.0);

        Ruby::attach(|rb| {
            let s = match &return_dtype {
                Some(DataType::Object(_)) => {
                    // If the return dtype is Object we should not go through AnyValue.
                    call_and_collect_objects(
                        rb,
                        series.name().clone(),
                        function,
                        series.len(),
                        series.iter().map(|av| av.null_to_none().map(Wrap)),
                        skip_nulls,
                    )
                }
                Some(return_dtype) => {
                    apply_all_polars_dtypes!(
                        series,
                        apply_generic_with_dtype,
                        rb,
                        function,
                        return_dtype,
                        skip_nulls
                    )
                }
                None => apply_all_polars_dtypes!(series, apply_generic, rb, function, skip_nulls),
            };
            s.map(RbSeries::from)
        })
    }
}

fn call_and_collect_objects<T, I>(
    rb: &Ruby,
    name: PlSmallStr,
    lambda: Value,
    len: usize,
    iter: I,
    skip_nulls: bool,
) -> RbResult<Series>
where
    T: IntoValue,
    I: Iterator<Item = Option<T>>,
{
    let mut objects = Vec::with_capacity(len);
    for opt_val in iter {
        let arg = match opt_val {
            None if skip_nulls => {
                objects.push(ObjectValue {
                    inner: Opaque::from(rb.qnil().into_value_with(rb)),
                });
                continue;
            }
            None => rb.qnil().into_value_with(rb),
            Some(val) => val.into_value_with(rb),
        };
        let out: Value = lambda.funcall("call", (arg,))?;
        objects.push(ObjectValue {
            inner: Opaque::from(out),
        });
    }
    Ok(series_from_objects(rb, name, objects))
}

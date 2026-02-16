use magnus::{IntoValue, RArray, Ruby, TryConvert, Value, value::Opaque, value::ReprValue};
use polars::frame::row::{Row, rows_to_schema_first_non_null};
use polars_core::utils::CustomIterTools;

use super::*;
use crate::error::RbPolarsErr;
use crate::prelude::*;
use crate::series::construction::series_from_objects;
use crate::{RbResult, RbSeries, raise_err};

impl RbDataFrame {
    pub fn map_rows(
        rb: &Ruby,
        self_: &Self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        inference_size: usize,
    ) -> RbResult<(Value, bool)> {
        let df = self_.df.read();
        let height = df.height();
        let col_series: Vec<_> = df
            .columns()
            .iter()
            .map(|s| s.as_materialized_series().clone())
            .collect();
        let mut iters: Vec<_> = col_series.iter().map(|c| c.iter()).collect();
        drop(df); // Release lock before calling lambda.

        let lambda_result_iter = (0..height).map(move |_| {
            let iter = iters.iter_mut().map(|it| Wrap(it.next().unwrap()));
            let tpl = rb.ary_from_iter(iter);
            lambda.funcall::<_, _, Value>("call", (tpl,))
        });

        // Simple case: return type set.
        if let Some(output_type) = &output_type {
            // If the output type is Object we should not go through AnyValue.
            if let DataType::Object(_) = output_type.0 {
                let objects = lambda_result_iter
                    .map(|res| {
                        Ok(ObjectValue {
                            inner: Opaque::from(res?),
                        })
                    })
                    .collect::<RbResult<Vec<_>>>()?;
                let s = series_from_objects(rb, PlSmallStr::from_static("map"), objects);
                return Ok((RbSeries::from(s).into_value_with(rb), false));
            }

            let avs = lambda_result_iter
                .map(|res| Wrap::<AnyValue>::try_convert(res?).map(|w| w.0))
                .collect::<RbResult<Vec<AnyValue>>>()?;
            let s = Series::from_any_values_and_dtype(
                PlSmallStr::from_static("map"),
                &avs,
                &output_type.0,
                true,
            )
            .map_err(RbPolarsErr::from)?;
            return Ok((RbSeries::from(s).into_value_with(rb), false));
        }

        // Disambiguate returning a DataFrame vs Series by checking the
        // first non-null output value.
        let mut peek_iter = lambda_result_iter.peekable();
        let mut null_count = 0;
        while let Some(ret) = peek_iter.peek() {
            if let Ok(v) = ret
                && v.is_nil()
            {
                null_count += 1;
                peek_iter.next();
            } else {
                break;
            }
        }

        let first_val = match peek_iter.peek() {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Err(e.clone()),
            None => {
                let msg = "The output type of the 'map_rows' function cannot be determined.\n\
                All returned values are None, consider setting the 'return_dtype'.";
                raise_err!(msg, ComputeError)
            }
        };

        if let Ok(first_row) = RArray::try_convert(*first_val) {
            let width = first_row.len();
            let out_df = collect_lambda_ret_with_rows_output(
                height,
                width,
                null_count,
                inference_size,
                peek_iter,
            )
            .map_err(RbPolarsErr::from)?;
            Ok((RbDataFrame::from(out_df).into_value_with(rb), true))
        } else {
            let avs = peek_iter
                .map(|res| Wrap::<AnyValue>::try_convert(res?).map(|w| w.0))
                .collect::<RbResult<Vec<AnyValue>>>()?;
            let s = Series::from_any_values(PlSmallStr::from_static("map"), &avs, true)
                .map_err(RbPolarsErr::from)?;

            let out = if null_count > 0 {
                let mut tmp = Series::full_null(s.name().clone(), null_count, s.dtype());
                tmp.append_owned(s).map_err(RbPolarsErr::from)?;
                tmp
            } else {
                s
            };
            Ok((RbSeries::from(out).into_value_with(rb), false))
        }
    }
}

fn collect_lambda_ret_with_rows_output(
    height: usize,
    width: usize,
    init_null_count: usize,
    inference_size: usize,
    ret_iter: impl Iterator<Item = RbResult<Value>>,
) -> PolarsResult<DataFrame> {
    let null_row = Row::new(vec![AnyValue::Null; width]);

    let mut row_buf = Row::default();
    let mut row_iter = ret_iter.map(|retval| {
        let retval = retval.unwrap();
        if retval.is_nil() {
            Ok(&null_row)
        } else {
            let tuple = RArray::try_convert(retval).map_err(|_| polars_err!(ComputeError: format!("expected tuple, got {}", unsafe { retval.classname() })))?;
            row_buf.0.clear();
            for v in tuple {
                let v = Wrap::<AnyValue>::try_convert(v).unwrap().0;
                row_buf.0.push(v);
            }
            let ptr = &row_buf as *const Row;
            // SAFETY:
            // we know that row constructor of polars dataframe does not keep a reference
            // to the row. Before we mutate the row buf again, the reference is dropped.
            // we only cannot prove it to the compiler.
            // we still to this because it save a Vec allocation in a hot loop.
            Ok(unsafe { &*ptr })
        }
    });

    // First rows for schema inference.
    let mut buf = Vec::with_capacity(inference_size);
    for v in (&mut row_iter).take(inference_size) {
        buf.push(v?.clone());
    }

    let schema = rows_to_schema_first_non_null(&buf, Some(50))?;

    if init_null_count > 0 {
        // SAFETY: we know the iterators size.
        let iter = unsafe {
            (0..init_null_count)
                .map(|_| Ok(&null_row))
                .chain(buf.iter().map(Ok))
                .chain(row_iter)
                .trust_my_length(height)
        };
        DataFrame::try_from_rows_iter_and_schema(iter, &schema)
    } else {
        // SAFETY: we know the iterators size.
        let iter = unsafe { buf.iter().map(Ok).chain(row_iter).trust_my_length(height) };
        DataFrame::try_from_rows_iter_and_schema(iter, &schema)
    }
}

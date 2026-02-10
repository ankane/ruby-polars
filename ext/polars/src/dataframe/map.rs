use magnus::{Ruby, Value, value::ReprValue};

use super::*;
use crate::map::dataframe::{
    apply_lambda_unknown, apply_lambda_with_bool_out_type, apply_lambda_with_primitive_out_type,
    apply_lambda_with_utf8_out_type,
};
use crate::prelude::*;
use crate::{RbResult, RbSeries};

impl RbDataFrame {
    pub fn map_rows(
        ruby: &Ruby,
        self_: &Self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        inference_size: usize,
    ) -> RbResult<(Value, bool)> {
        let df = &self_.df.read();

        let output_type = output_type.map(|dt| dt.0);
        let out = match output_type {
            Some(DataType::Int32) => {
                apply_lambda_with_primitive_out_type::<Int32Type>(df, lambda, 0, None).into_series()
            }
            Some(DataType::Int64) => {
                apply_lambda_with_primitive_out_type::<Int64Type>(df, lambda, 0, None).into_series()
            }
            Some(DataType::UInt32) => {
                apply_lambda_with_primitive_out_type::<UInt32Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::UInt64) => {
                apply_lambda_with_primitive_out_type::<UInt64Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::Float16) => todo!(),
            Some(DataType::Float32) => {
                apply_lambda_with_primitive_out_type::<Float32Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::Float64) => {
                apply_lambda_with_primitive_out_type::<Float64Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::Boolean) => {
                apply_lambda_with_bool_out_type(df, lambda, 0, None).into_series()
            }
            Some(DataType::Date) => {
                apply_lambda_with_primitive_out_type::<Int32Type>(df, lambda, 0, None)
                    .into_date()
                    .into_series()
            }
            Some(DataType::Datetime(tu, tz)) => {
                apply_lambda_with_primitive_out_type::<Int64Type>(df, lambda, 0, None)
                    .into_datetime(tu, tz)
                    .into_series()
            }
            Some(DataType::String) => {
                apply_lambda_with_utf8_out_type(df, lambda, 0, None).into_series()
            }
            _ => return apply_lambda_unknown(df, lambda, inference_size),
        };

        Ok((ruby.obj_wrap(RbSeries::from(out)).as_value(), false))
    }
}

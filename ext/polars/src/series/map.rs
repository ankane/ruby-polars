use magnus::Value;

use super::RbSeries;
use crate::RbResult;
use crate::apply_method_all_arrow_series2;
use crate::map::series::{ApplyLambda, call_lambda_and_extract};
use crate::prelude::*;

impl RbSeries {
    pub fn map_elements(
        &self,
        lambda: Value,
        return_dtype: Option<Wrap<DataType>>,
        skip_nulls: bool,
    ) -> RbResult<Self> {
        let series = &self.series.borrow();

        let return_dtype = return_dtype.map(|dt| dt.0);

        macro_rules! dispatch_apply {
            ($self:expr, $method:ident, $($args:expr),*) => {
                if matches!($self.dtype(), DataType::Object(_)) {
                    // let ca = $self.0.unpack::<ObjectType<ObjectValue>>().unwrap();
                    // ca.$method($($args),*)
                    todo!()
                } else {
                    apply_method_all_arrow_series2!(
                        $self,
                        $method,
                        $($args),*
                    )
                }

            }

        }

        if matches!(
            series.dtype(),
            DataType::Datetime(_, _)
                | DataType::Date
                | DataType::Duration(_)
                | DataType::Categorical(_, _)
                | DataType::Time
        ) || !skip_nulls
        {
            let mut avs = Vec::with_capacity(series.len());
            let iter = series.iter().map(|av| {
                let input = Wrap(av);
                call_lambda_and_extract::<_, Wrap<AnyValue>>(lambda, input)
                    .unwrap()
                    .0
            });
            avs.extend(iter);
            return Ok(Series::new(self.name().into(), &avs).into());
        }

        let out = match return_dtype {
            Some(DataType::Int8) => {
                let ca: Int8Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Int16) => {
                let ca: Int16Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Int32) => {
                let ca: Int32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Int64) => {
                let ca: Int64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt8) => {
                let ca: UInt8Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt16) => {
                let ca: UInt16Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt32) => {
                let ca: UInt32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt64) => {
                let ca: UInt64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Float32) => {
                let ca: Float32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Float64) => {
                let ca: Float64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Boolean) => {
                let ca: BooleanChunked =
                    dispatch_apply!(series, apply_lambda_with_bool_out_type, lambda, 0, None)?;
                ca.into_series()
            }
            Some(DataType::Date) => {
                let ca: Int32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_date().into_series()
            }
            Some(DataType::Datetime(tu, tz)) => {
                let ca: Int64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_datetime(tu, tz).into_series()
            }
            Some(DataType::String) => {
                let ca = dispatch_apply!(series, apply_lambda_with_utf8_out_type, lambda, 0, None)?;

                ca.into_series()
            }
            Some(DataType::Object(_)) => {
                let ca =
                    dispatch_apply!(series, apply_lambda_with_object_out_type, lambda, 0, None)?;
                ca.into_series()
            }
            None => return dispatch_apply!(series, apply_lambda_unknown, lambda),

            _ => return dispatch_apply!(series, apply_lambda_unknown, lambda),
        };

        Ok(RbSeries::new(out))
    }
}

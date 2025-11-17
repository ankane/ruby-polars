use magnus::{Ruby, Value};

use super::RbSeries;
use crate::map::check_nested_object;
use crate::map::series::{ApplyLambda, call_lambda_and_extract};
use crate::prelude::*;
use crate::utils::RubyAttach;
use crate::{RbPolarsErr, RbResult};
use crate::{apply_method_all_arrow_series2, raise_err};

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

        macro_rules! dispatch_apply {
            ($self:expr, $method:ident, $($args:expr),*) => {
                match $self.dtype() {
                    DataType::Object(_) => {
                        // let ca = $self.0.unpack::<ObjectType<ObjectValue>>().unwrap();
                        // ca.$method($($args),*)
                        todo!()
                    }
                    _ => {
                        apply_method_all_arrow_series2!(
                            $self,
                            $method,
                            $($args),*
                        )
                    }
                }
            }

        }

        Ruby::attach(|_rb| {
            if matches!(
                series.dtype(),
                DataType::Datetime(_, _)
                    | DataType::Date
                    | DataType::Duration(_)
                    | DataType::Categorical(_, _)
                    | DataType::Enum(_, _)
                    | DataType::Binary
                    | DataType::Array(_, _)
                    | DataType::Time
                    | DataType::Decimal(_, _)
            ) || !skip_nulls
            {
                let mut avs = Vec::with_capacity(series.len());
                let s = series.rechunk();

                for av in s.iter() {
                    let out = match (skip_nulls, av) {
                        (true, AnyValue::Null) => AnyValue::Null,
                        (_, av) => {
                            let av: Option<Wrap<AnyValue>> =
                                call_lambda_and_extract(function, Wrap(av))?;
                            match av {
                                None => AnyValue::Null,
                                Some(av) => av.0,
                            }
                        }
                    };
                    avs.push(out)
                }
                let out = Series::new(series.name().clone(), &avs);
                let dtype = out.dtype();
                if dtype.is_nested() {
                    check_nested_object(dtype)?;
                }

                return Ok(out.into());
            }

            let out = match return_dtype {
                Some(DataType::Int8) => {
                    let ca: Int8Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::Int16) => {
                    let ca: Int16Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::Int32) => {
                    let ca: Int32Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::Int64) => {
                    let ca: Int64Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::UInt8) => {
                    let ca: UInt8Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::UInt16) => {
                    let ca: UInt16Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::UInt32) => {
                    let ca: UInt32Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::UInt64) => {
                    let ca: UInt64Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::Float32) => {
                    let ca: Float32Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::Float64) => {
                    let ca: Float64Chunked = dispatch_apply!(
                        series,
                        apply_lambda_with_primitive_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::Boolean) => {
                    let ca: BooleanChunked = dispatch_apply!(
                        series,
                        apply_lambda_with_bool_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                Some(DataType::String) => {
                    let ca = dispatch_apply!(
                        series,
                        apply_lambda_with_utf8_out_type,
                        function,
                        0,
                        None
                    )?;

                    ca.into_series()
                }
                Some(DataType::List(_inner)) => {
                    todo!()
                }
                Some(DataType::Object(_)) => {
                    let ca = dispatch_apply!(
                        series,
                        apply_lambda_with_object_out_type,
                        function,
                        0,
                        None
                    )?;
                    ca.into_series()
                }
                None => return dispatch_apply!(series, apply_lambda_unknown, function),

                _ => return dispatch_apply!(series, apply_lambda_unknown, function),
            };

            Ok(RbSeries::new(out))
        })
    }
}

use magnus::{class, prelude::*, typed_data::Obj, IntoValue, RHash, TryConvert, Value};
use polars::prelude::*;

use super::*;
use crate::conversion::slice_to_wrapped;
use crate::series::RbSeries;
use crate::{ObjectValue, RbResult};

/// Find the output type and dispatch to that implementation.
fn infer_and_finish<'a, A: ApplyLambda<'a>>(
    applyer: &'a A,
    lambda: Value,
    out: Value,
    null_count: usize,
) -> RbResult<RbSeries> {
    if out.is_kind_of(class::true_class()) || out.is_kind_of(class::false_class()) {
        let first_value = bool::try_convert(out).unwrap();
        applyer
            .apply_lambda_with_bool_out_type(lambda, null_count, Some(first_value))
            .map(|ca| ca.into_series().into())
    } else if out.is_kind_of(class::float()) {
        let first_value = f64::try_convert(out).unwrap();
        applyer
            .apply_lambda_with_primitive_out_type::<Float64Type>(
                lambda,
                null_count,
                Some(first_value),
            )
            .map(|ca| ca.into_series().into())
    } else if out.is_kind_of(class::string()) {
        let first_value = String::try_convert(out).unwrap();
        applyer
            .apply_lambda_with_utf8_out_type(lambda, null_count, Some(first_value.as_str()))
            .map(|ca| ca.into_series().into())
    } else if out.respond_to("_s", true)? {
        todo!()
    } else if out.is_kind_of(class::array()) {
        todo!()
    } else if out.is_kind_of(class::hash()) {
        let first = Wrap::<AnyValue<'_>>::try_convert(out)?;
        applyer.apply_to_struct(lambda, null_count, first.0)
    }
    // this succeeds for numpy ints as well, where checking if it is pyint fails
    // we do this later in the chain so that we don't extract integers from string chars.
    else if i64::try_convert(out).is_ok() {
        let first_value = i64::try_convert(out).unwrap();
        applyer
            .apply_lambda_with_primitive_out_type::<Int64Type>(
                lambda,
                null_count,
                Some(first_value),
            )
            .map(|ca| ca.into_series().into())
    } else if let Ok(av) = Wrap::<AnyValue>::try_convert(out) {
        applyer
            .apply_extract_any_values(lambda, null_count, av.0)
            .map(|s| s.into())
    } else {
        applyer
            .apply_lambda_with_object_out_type(lambda, null_count, Some(out.into()))
            .map(|ca| ca.into_series().into())
    }
}

pub trait ApplyLambda<'a> {
    fn apply_lambda_unknown(&'a self, _lambda: Value) -> RbResult<RbSeries>;

    /// Apply a lambda that doesn't change output types
    fn apply_lambda(&'a self, _lambda: Value) -> RbResult<RbSeries>;

    // Used to store a struct type
    fn apply_to_struct(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<RbSeries>;

    /// Apply a lambda with a primitive output type
    fn apply_lambda_with_primitive_out_type<D>(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<D::Native>,
    ) -> RbResult<ChunkedArray<D>>
    where
        D: RbArrowPrimitiveType,
        D::Native: IntoValue + TryConvert;

    /// Apply a lambda with a boolean output type
    fn apply_lambda_with_bool_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<bool>,
    ) -> RbResult<ChunkedArray<BooleanType>>;

    /// Apply a lambda with utf8 output type
    fn apply_lambda_with_utf8_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<&str>,
    ) -> RbResult<Utf8Chunked>;

    /// Apply a lambda with list output type
    fn apply_lambda_with_list_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: &Series,
        dt: &DataType,
    ) -> RbResult<ListChunked>;

    fn apply_extract_any_values(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<Series>;

    /// Apply a lambda with list output type
    fn apply_lambda_with_object_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<ObjectValue>,
    ) -> RbResult<ObjectChunked<ObjectValue>>;
}

pub fn call_lambda<T>(lambda: Value, in_val: T) -> RbResult<Value>
where
    T: IntoValue,
{
    lambda.funcall("call", (in_val,))
}

pub(crate) fn call_lambda_and_extract<T, S>(lambda: Value, in_val: T) -> RbResult<S>
where
    T: IntoValue,
    S: TryConvert,
{
    match call_lambda(lambda, in_val) {
        Ok(out) => S::try_convert(out),
        Err(e) => panic!("ruby function failed {}", e),
    }
}

fn call_lambda_series_out<T>(lambda: Value, in_val: T) -> RbResult<Series>
where
    T: IntoValue,
{
    let out: Value = lambda.funcall("call", (in_val,))?;
    let py_series: Obj<RbSeries> = out.funcall("_s", ())?;
    let tmp = py_series.series.borrow();
    Ok(tmp.clone())
}

impl<'a> ApplyLambda<'a> for BooleanChunked {
    fn apply_lambda_unknown(&'a self, lambda: Value) -> RbResult<RbSeries> {
        let mut null_count = 0;
        for opt_v in self.into_iter() {
            if let Some(v) = opt_v {
                let arg = (v,);
                let out: Value = lambda.funcall("call", arg)?;
                if out.is_nil() {
                    null_count += 1;
                    continue;
                }
                return infer_and_finish(self, lambda, out, null_count);
            } else {
                null_count += 1
            }
        }
        Ok(Self::full_null(self.name(), self.len())
            .into_series()
            .into())
    }

    fn apply_lambda(&'a self, lambda: Value) -> RbResult<RbSeries> {
        self.apply_lambda_with_bool_out_type(lambda, 0, None)
            .map(|ca| RbSeries::new(ca.into_series()))
    }

    fn apply_to_struct(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<RbSeries> {
        let skip = 1;
        if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda(lambda, val).ok());
            iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda(lambda, val).ok()));
            iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
        }
    }

    fn apply_lambda_with_primitive_out_type<D>(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<D::Native>,
    ) -> RbResult<ChunkedArray<D>>
    where
        D: RbArrowPrimitiveType,
        D::Native: IntoValue + TryConvert,
    {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());
            Ok(iterator_to_primitive(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_primitive(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_bool_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<bool>,
    ) -> RbResult<BooleanChunked> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());
            Ok(iterator_to_bool(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_bool(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_utf8_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<&str>,
    ) -> RbResult<Utf8Chunked> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());

            Ok(iterator_to_utf8(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_utf8(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_list_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: &Series,
        dt: &DataType,
    ) -> RbResult<ListChunked> {
        let skip = 1;
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_series_out(lambda, val).ok());

            iterator_to_list(
                dt,
                it,
                init_null_count,
                Some(first_value),
                self.name(),
                self.len(),
            )
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_series_out(lambda, val).ok()));
            iterator_to_list(
                dt,
                it,
                init_null_count,
                Some(first_value),
                self.name(),
                self.len(),
            )
        }
    }

    fn apply_extract_any_values(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<Series> {
        let mut avs = Vec::with_capacity(self.len());
        avs.extend(std::iter::repeat(AnyValue::Null).take(init_null_count));
        avs.push(first_value);

        if self.null_count() > 0 {
            let iter = self.into_iter().skip(init_null_count + 1).map(|opt_val| {
                let out_wrapped = match opt_val {
                    None => Wrap(AnyValue::Null),
                    Some(val) => call_lambda_and_extract(lambda, val).unwrap(),
                };
                out_wrapped.0
            });
            avs.extend(iter);
        } else {
            let iter = self
                .into_no_null_iter()
                .skip(init_null_count + 1)
                .map(|val| {
                    call_lambda_and_extract::<_, Wrap<AnyValue>>(lambda, val)
                        .unwrap()
                        .0
                });
            avs.extend(iter);
        }
        Ok(Series::new(self.name(), &avs))
    }

    fn apply_lambda_with_object_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<ObjectValue>,
    ) -> RbResult<ObjectChunked<ObjectValue>> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());

            Ok(iterator_to_object(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_object(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }
}

impl<'a, T> ApplyLambda<'a> for ChunkedArray<T>
where
    T: RbArrowPrimitiveType + PolarsNumericType,
    T::Native: IntoValue + TryConvert,
    ChunkedArray<T>: IntoSeries,
{
    fn apply_lambda_unknown(&'a self, lambda: Value) -> RbResult<RbSeries> {
        let mut null_count = 0;
        for opt_v in self.into_iter() {
            if let Some(v) = opt_v {
                let arg = (v,);
                let out: Value = lambda.funcall("call", arg)?;
                if out.is_nil() {
                    null_count += 1;
                    continue;
                }
                return infer_and_finish(self, lambda, out, null_count);
            } else {
                null_count += 1
            }
        }
        Ok(Self::full_null(self.name(), self.len())
            .into_series()
            .into())
    }

    fn apply_lambda(&'a self, lambda: Value) -> RbResult<RbSeries> {
        self.apply_lambda_with_primitive_out_type::<T>(lambda, 0, None)
            .map(|ca| RbSeries::new(ca.into_series()))
    }

    fn apply_to_struct(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<RbSeries> {
        let skip = 1;
        if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda(lambda, val).ok());
            iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda(lambda, val).ok()));
            iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
        }
    }

    fn apply_lambda_with_primitive_out_type<D>(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<D::Native>,
    ) -> RbResult<ChunkedArray<D>>
    where
        D: RbArrowPrimitiveType,
        D::Native: IntoValue + TryConvert,
    {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());
            Ok(iterator_to_primitive(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_primitive(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_bool_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<bool>,
    ) -> RbResult<BooleanChunked> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());
            Ok(iterator_to_bool(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_bool(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_utf8_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<&str>,
    ) -> RbResult<Utf8Chunked> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());

            Ok(iterator_to_utf8(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_utf8(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_list_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: &Series,
        dt: &DataType,
    ) -> RbResult<ListChunked> {
        let skip = 1;
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_series_out(lambda, val).ok());

            iterator_to_list(
                dt,
                it,
                init_null_count,
                Some(first_value),
                self.name(),
                self.len(),
            )
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_series_out(lambda, val).ok()));
            iterator_to_list(
                dt,
                it,
                init_null_count,
                Some(first_value),
                self.name(),
                self.len(),
            )
        }
    }

    fn apply_extract_any_values(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<Series> {
        let mut avs = Vec::with_capacity(self.len());
        avs.extend(std::iter::repeat(AnyValue::Null).take(init_null_count));
        avs.push(first_value);

        if self.null_count() > 0 {
            let iter = self.into_iter().skip(init_null_count + 1).map(|opt_val| {
                let out_wrapped = match opt_val {
                    None => Wrap(AnyValue::Null),
                    Some(val) => call_lambda_and_extract(lambda, val).unwrap(),
                };
                out_wrapped.0
            });
            avs.extend(iter);
        } else {
            let iter = self
                .into_no_null_iter()
                .skip(init_null_count + 1)
                .map(|val| {
                    call_lambda_and_extract::<_, Wrap<AnyValue>>(lambda, val)
                        .unwrap()
                        .0
                });
            avs.extend(iter);
        }
        Ok(Series::new(self.name(), &avs))
    }

    fn apply_lambda_with_object_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<ObjectValue>,
    ) -> RbResult<ObjectChunked<ObjectValue>> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());

            Ok(iterator_to_object(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_object(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }
}

impl<'a> ApplyLambda<'a> for Utf8Chunked {
    fn apply_lambda_unknown(&'a self, lambda: Value) -> RbResult<RbSeries> {
        let mut null_count = 0;
        for opt_v in self.into_iter() {
            if let Some(v) = opt_v {
                let arg = (v,);
                let out: Value = lambda.funcall("call", arg)?;
                if out.is_nil() {
                    null_count += 1;
                    continue;
                }
                return infer_and_finish(self, lambda, out, null_count);
            } else {
                null_count += 1
            }
        }
        Ok(Self::full_null(self.name(), self.len())
            .into_series()
            .into())
    }

    fn apply_lambda(&'a self, lambda: Value) -> RbResult<RbSeries> {
        let ca = self.apply_lambda_with_utf8_out_type(lambda, 0, None)?;
        Ok(ca.into_series().into())
    }

    fn apply_to_struct(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<RbSeries> {
        let skip = 1;
        if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda(lambda, val).ok());
            iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda(lambda, val).ok()));
            iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
        }
    }

    fn apply_lambda_with_primitive_out_type<D>(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<D::Native>,
    ) -> RbResult<ChunkedArray<D>>
    where
        D: RbArrowPrimitiveType,
        D::Native: IntoValue + TryConvert,
    {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());
            Ok(iterator_to_primitive(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_primitive(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_bool_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<bool>,
    ) -> RbResult<BooleanChunked> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());
            Ok(iterator_to_bool(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_bool(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_utf8_out_type(
        &self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<&str>,
    ) -> RbResult<Utf8Chunked> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());

            Ok(iterator_to_utf8(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_utf8(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }

    fn apply_lambda_with_list_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: &Series,
        dt: &DataType,
    ) -> RbResult<ListChunked> {
        let skip = 1;
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_series_out(lambda, val).ok());

            iterator_to_list(
                dt,
                it,
                init_null_count,
                Some(first_value),
                self.name(),
                self.len(),
            )
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_series_out(lambda, val).ok()));
            iterator_to_list(
                dt,
                it,
                init_null_count,
                Some(first_value),
                self.name(),
                self.len(),
            )
        }
    }

    fn apply_extract_any_values(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<Series> {
        let mut avs = Vec::with_capacity(self.len());
        avs.extend(std::iter::repeat(AnyValue::Null).take(init_null_count));
        avs.push(first_value);

        if self.null_count() > 0 {
            let iter = self.into_iter().skip(init_null_count + 1).map(|opt_val| {
                let out_wrapped = match opt_val {
                    None => Wrap(AnyValue::Null),
                    Some(val) => call_lambda_and_extract(lambda, val).unwrap(),
                };
                out_wrapped.0
            });
            avs.extend(iter);
        } else {
            let iter = self
                .into_no_null_iter()
                .skip(init_null_count + 1)
                .map(|val| {
                    call_lambda_and_extract::<_, Wrap<AnyValue>>(lambda, val)
                        .unwrap()
                        .0
                });
            avs.extend(iter);
        }
        Ok(Series::new(self.name(), &avs))
    }

    fn apply_lambda_with_object_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<ObjectValue>,
    ) -> RbResult<ObjectChunked<ObjectValue>> {
        let skip = usize::from(first_value.is_some());
        if init_null_count == self.len() {
            Ok(ChunkedArray::full_null(self.name(), self.len()))
        } else if !self.has_validity() {
            let it = self
                .into_no_null_iter()
                .skip(init_null_count + skip)
                .map(|val| call_lambda_and_extract(lambda, val).ok());

            Ok(iterator_to_object(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        } else {
            let it = self
                .into_iter()
                .skip(init_null_count + skip)
                .map(|opt_val| opt_val.and_then(|val| call_lambda_and_extract(lambda, val).ok()));
            Ok(iterator_to_object(
                it,
                init_null_count,
                first_value,
                self.name(),
                self.len(),
            ))
        }
    }
}

fn make_dict_arg(names: &[&str], vals: &[AnyValue]) -> RHash {
    let dict = RHash::new();
    for (name, val) in names.iter().zip(slice_to_wrapped(vals)) {
        dict.aset(name.to_string(), (*val).clone()).unwrap()
    }
    dict
}

impl<'a> ApplyLambda<'a> for StructChunked {
    fn apply_lambda_unknown(&'a self, lambda: Value) -> RbResult<RbSeries> {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();
        let mut null_count = 0;
        for val in self.into_iter() {
            let arg = make_dict_arg(&names, val);
            let out: Value = lambda.funcall("call", (arg,))?;
            if out.is_nil() {
                null_count += 1;
                continue;
            }
            return infer_and_finish(self, lambda, out, null_count);
        }

        // todo! full null
        Ok(self.clone().into_series().into())
    }

    fn apply_lambda(&'a self, lambda: Value) -> RbResult<RbSeries> {
        self.apply_lambda_unknown(lambda)
    }

    fn apply_to_struct(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<RbSeries> {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();

        let skip = 1;
        let it = self.into_iter().skip(init_null_count + skip).map(|val| {
            let arg = make_dict_arg(&names, val);
            let out = lambda.funcall("call", (arg,)).unwrap();
            Some(out)
        });
        iterator_to_struct(it, init_null_count, first_value, self.name(), self.len())
    }

    fn apply_lambda_with_primitive_out_type<D>(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<D::Native>,
    ) -> RbResult<ChunkedArray<D>>
    where
        D: RbArrowPrimitiveType,
        D::Native: IntoValue + TryConvert,
    {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();

        let skip = usize::from(first_value.is_some());
        let it = self.into_iter().skip(init_null_count + skip).map(|val| {
            let arg = make_dict_arg(&names, val);
            call_lambda_and_extract(lambda, arg).ok()
        });

        Ok(iterator_to_primitive(
            it,
            init_null_count,
            first_value,
            self.name(),
            self.len(),
        ))
    }

    fn apply_lambda_with_bool_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<bool>,
    ) -> RbResult<BooleanChunked> {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();

        let skip = usize::from(first_value.is_some());
        let it = self.into_iter().skip(init_null_count + skip).map(|val| {
            let arg = make_dict_arg(&names, val);
            call_lambda_and_extract(lambda, arg).ok()
        });

        Ok(iterator_to_bool(
            it,
            init_null_count,
            first_value,
            self.name(),
            self.len(),
        ))
    }

    fn apply_lambda_with_utf8_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<&str>,
    ) -> RbResult<Utf8Chunked> {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();

        let skip = usize::from(first_value.is_some());
        let it = self.into_iter().skip(init_null_count + skip).map(|val| {
            let arg = make_dict_arg(&names, val);
            call_lambda_and_extract(lambda, arg).ok()
        });

        Ok(iterator_to_utf8(
            it,
            init_null_count,
            first_value,
            self.name(),
            self.len(),
        ))
    }

    fn apply_lambda_with_list_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: &Series,
        dt: &DataType,
    ) -> RbResult<ListChunked> {
        let skip = 1;

        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();

        let it = self.into_iter().skip(init_null_count + skip).map(|val| {
            let arg = make_dict_arg(&names, val);
            call_lambda_series_out(lambda, arg).ok()
        });
        iterator_to_list(
            dt,
            it,
            init_null_count,
            Some(first_value),
            self.name(),
            self.len(),
        )
    }

    fn apply_extract_any_values(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: AnyValue<'a>,
    ) -> RbResult<Series> {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();
        let mut avs = Vec::with_capacity(self.len());
        avs.extend(std::iter::repeat(AnyValue::Null).take(init_null_count));
        avs.push(first_value);

        let iter = self.into_iter().skip(init_null_count + 1).map(|val| {
            let arg = make_dict_arg(&names, val);
            call_lambda_and_extract::<_, Wrap<AnyValue>>(lambda, arg)
                .unwrap()
                .0
        });
        avs.extend(iter);

        Ok(Series::new(self.name(), &avs))
    }

    fn apply_lambda_with_object_out_type(
        &'a self,
        lambda: Value,
        init_null_count: usize,
        first_value: Option<ObjectValue>,
    ) -> RbResult<ObjectChunked<ObjectValue>> {
        let names = self.fields().iter().map(|s| s.name()).collect::<Vec<_>>();

        let skip = usize::from(first_value.is_some());
        let it = self.into_iter().skip(init_null_count + skip).map(|val| {
            let arg = make_dict_arg(&names, val);
            call_lambda_and_extract(lambda, arg).ok()
        });

        Ok(iterator_to_object(
            it,
            init_null_count,
            first_value,
            self.name(),
            self.len(),
        ))
    }
}

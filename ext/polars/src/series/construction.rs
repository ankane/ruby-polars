use arrow::bitmap::BitmapBuilder;
use magnus::{RArray, RString, Ruby, Value, prelude::*};
use num_traits::AsPrimitive;
use polars::prelude::*;

use crate::any_value::rb_object_to_any_value;
use crate::conversion::{Wrap, slice_extract_wrapped, vec_extract_wrapped};
use crate::prelude::ObjectValue;
use crate::{RbPolarsErr, RbResult, RbSeries, RbTypeError, RbValueError};

pub fn series_from_objects(rb: &Ruby, name: PlSmallStr, objects: Vec<ObjectValue>) -> Series {
    let mut validity = BitmapBuilder::with_capacity(objects.len());
    for v in &objects {
        let is_valid = !rb.get_inner(v.inner).is_nil();
        // SAFETY: we can ensure that validity has correct capacity.
        unsafe { validity.push_unchecked(is_valid) };
    }
    ObjectChunked::<ObjectValue>::new_from_vec_and_validity(
        name,
        objects,
        validity.into_opt_validity(),
    )
    .into_series()
}

impl RbSeries {
    pub fn new_opt_bool(name: String, obj: RArray, strict: bool) -> RbResult<RbSeries> {
        let len = obj.len();
        let mut builder = BooleanChunkedBuilder::new(name.into(), len);

        unsafe {
            for item in obj.as_slice().iter() {
                if item.is_nil() {
                    builder.append_null()
                } else {
                    match bool::try_convert(*item) {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            if strict {
                                return Err(e);
                            }
                            builder.append_null()
                        }
                    }
                }
            }
        }
        let ca = builder.finish();

        let s = ca.into_series();
        Ok(RbSeries::new(s))
    }
}

fn new_primitive<T, F>(name: &str, values: RArray, _strict: bool, extract: F) -> RbResult<RbSeries>
where
    T: PolarsNumericType,
    F: Fn(Value) -> RbResult<T::Native>,
{
    let len = values.len();
    let mut builder = PrimitiveChunkedBuilder::<T>::new(name.into(), len);

    for res in values.into_iter() {
        let value = res;
        if value.is_nil() {
            builder.append_null()
        } else {
            let v = extract(value)?;
            builder.append_value(v)
        }
    }

    let ca = builder.finish();
    let s = ca.into_series();
    Ok(s.into())
}

// Init with lists that can contain Nones
macro_rules! init_method_opt {
    ($name:ident, $type:ty, $native: ty) => {
        impl RbSeries {
            pub fn $name(name: String, obj: RArray, strict: bool) -> RbResult<Self> {
                new_primitive::<$type, _>(&name, obj, strict, |v| <$native>::try_convert(v))
            }
        }
    };
}

init_method_opt!(new_opt_u8, UInt8Type, u8);
init_method_opt!(new_opt_u16, UInt16Type, u16);
init_method_opt!(new_opt_u32, UInt32Type, u32);
init_method_opt!(new_opt_u64, UInt64Type, u64);
init_method_opt!(new_opt_u128, UInt128Type, u128);
init_method_opt!(new_opt_i8, Int8Type, i8);
init_method_opt!(new_opt_i16, Int16Type, i16);
init_method_opt!(new_opt_i32, Int32Type, i32);
init_method_opt!(new_opt_i64, Int64Type, i64);
init_method_opt!(new_opt_i128, Int128Type, i128);
init_method_opt!(new_opt_f32, Float32Type, f32);
init_method_opt!(new_opt_f64, Float64Type, f64);

impl RbSeries {
    pub fn new_opt_f16(name: String, values: RArray, _strict: bool) -> RbResult<Self> {
        new_primitive::<Float16Type, _>(&name, values, false, |v| {
            Ok(AsPrimitive::<pf16>::as_(f64::try_convert(v)?))
        })
    }
}

fn vec_wrap_any_value<'s>(arr: RArray) -> RbResult<Vec<Wrap<AnyValue<'s>>>> {
    let mut val = Vec::with_capacity(arr.len());
    for v in arr.into_iter() {
        val.push(Wrap::<AnyValue<'s>>::try_convert(v)?);
    }
    Ok(val)
}

impl RbSeries {
    pub fn new_from_any_values(name: String, values: RArray, strict: bool) -> RbResult<Self> {
        let any_values_result = vec_wrap_any_value(values);
        // from anyvalues is fallible
        let result = any_values_result.and_then(|avs| {
            let avs = slice_extract_wrapped(&avs);
            let s = Series::from_any_values(name.clone().into(), avs, strict).map_err(|e| {
                RbTypeError::new_err(format!(
                    "{e}\n\nHint: Try setting `strict: false` to allow passing data with mixed types."
                ))
            })?;
            Ok(s.into())
        });

        // Fall back to Object type for non-strict construction.
        if !strict && result.is_err() {
            return Self::new_object(name, values, strict);
        }

        result
    }

    pub fn new_from_any_values_and_dtype(
        name: String,
        values: RArray,
        dtype: Wrap<DataType>,
        strict: bool,
    ) -> RbResult<Self> {
        let any_values = values
            .into_iter()
            .map(|v| rb_object_to_any_value(v, strict))
            .collect::<RbResult<Vec<AnyValue>>>()?;
        let s = Series::from_any_values_and_dtype(
            name.into(),
            any_values.as_slice(),
            &dtype.0,
            strict,
        )
        .map_err(|e| {
            RbTypeError::new_err(format!(
                "{e}\n\nHint: Try setting `strict: false` to allow passing data with mixed types."
            ))
        })?;
        Ok(s.into())
    }

    pub fn new_str(name: String, values: RArray, _strict: bool) -> RbResult<Self> {
        let len = values.len();
        let mut builder = StringChunkedBuilder::new(name.into(), len);

        for res in values.into_iter() {
            let value = res;
            if value.is_nil() {
                builder.append_null()
            } else {
                let v = String::try_convert(value)?;
                builder.append_value(v)
            }
        }

        let ca = builder.finish();
        let s = ca.into_series();
        Ok(s.into())
    }

    pub fn new_binary(name: String, values: RArray, _strict: bool) -> RbResult<Self> {
        let len = values.len();
        let mut builder = BinaryChunkedBuilder::new(name.into(), len);

        for res in values.into_iter() {
            let value = res;
            if value.is_nil() {
                builder.append_null()
            } else {
                let v = RString::try_convert(value)?;
                builder.append_value(unsafe { v.as_slice() })
            }
        }

        let ca = builder.finish();
        let s = ca.into_series();
        Ok(s.into())
    }

    pub fn new_null(name: String, values: RArray, _strict: bool) -> RbResult<Self> {
        let len = values.len();
        Ok(Series::new_null(name.into(), len).into())
    }

    pub fn new_object(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let val = val
            .into_iter()
            .map(ObjectValue::from)
            .collect::<Vec<ObjectValue>>();
        let s = ObjectChunked::<ObjectValue>::new_from_vec(name.into(), val).into_series();
        Ok(s.into())
    }

    pub fn new_series_list(name: String, values: RArray, _strict: bool) -> RbResult<Self> {
        let series: Vec<_> = values
            .into_iter()
            .map(|ops| {
                Option::<&RbSeries>::try_convert(ops)
                    .unwrap()
                    .map(|ps| ps.clone().series.into_inner())
            })
            .collect();
        if let Some(s) = series.iter().flatten().next()
            && s.dtype().is_object()
        {
            return Err(RbValueError::new_err(
                "list of objects isn't supported; try building a 'object' only series",
            ));
        }
        Ok(Series::new(name.into(), series).into())
    }

    pub fn new_array(
        width: usize,
        inner: Option<Wrap<DataType>>,
        name: String,
        val: RArray,
        _strict: bool,
    ) -> RbResult<Self> {
        let val = vec_wrap_any_value(val)?;
        let val = vec_extract_wrapped(val);
        let out = Series::new(name.into(), &val);
        match out.dtype() {
            DataType::List(list_inner) => {
                let out = out
                    .cast(&DataType::Array(
                        Box::new(inner.map(|dt| dt.0).unwrap_or(*list_inner.clone())),
                        width,
                    ))
                    .map_err(RbPolarsErr::from)?;
                Ok(out.into())
            }
            _ => Err(RbValueError::new_err(
                "could not create Array from input".to_string(),
            )),
        }
    }

    pub fn new_decimal(name: String, values: RArray, strict: bool) -> RbResult<Self> {
        Self::new_from_any_values(name, values, strict)
    }
}

use magnus::{prelude::*, RArray};
use polars_core::prelude::*;

use crate::conversion::{slice_extract_wrapped, vec_extract_wrapped, Wrap};
use crate::prelude::ObjectValue;
use crate::series::to_series_collection;
use crate::{RbPolarsErr, RbResult, RbSeries, RbTypeError, RbValueError};

impl RbSeries {
    pub fn new_opt_bool(name: String, obj: RArray, strict: bool) -> RbResult<RbSeries> {
        let len = obj.len();
        let mut builder = BooleanChunkedBuilder::new(&name, len);

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

fn new_primitive<T>(name: &str, obj: RArray, strict: bool) -> RbResult<RbSeries>
where
    T: PolarsNumericType,
    ChunkedArray<T>: IntoSeries,
    T::Native: magnus::TryConvert,
{
    let len = obj.len();
    let mut builder = PrimitiveChunkedBuilder::<T>::new(name, len);

    unsafe {
        for item in obj.as_slice().iter() {
            if item.is_nil() {
                builder.append_null()
            } else {
                match T::Native::try_convert(*item) {
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

// Init with lists that can contain Nones
macro_rules! init_method_opt {
    ($name:ident, $type:ty, $native: ty) => {
        impl RbSeries {
            pub fn $name(name: String, obj: RArray, strict: bool) -> RbResult<Self> {
                new_primitive::<$type>(&name, obj, strict)
            }
        }
    };
}

init_method_opt!(new_opt_u8, UInt8Type, u8);
init_method_opt!(new_opt_u16, UInt16Type, u16);
init_method_opt!(new_opt_u32, UInt32Type, u32);
init_method_opt!(new_opt_u64, UInt64Type, u64);
init_method_opt!(new_opt_i8, Int8Type, i8);
init_method_opt!(new_opt_i16, Int16Type, i16);
init_method_opt!(new_opt_i32, Int32Type, i32);
init_method_opt!(new_opt_i64, Int64Type, i64);
init_method_opt!(new_opt_f32, Float32Type, f32);
init_method_opt!(new_opt_f64, Float64Type, f64);

fn vec_wrap_any_value<'s>(arr: RArray) -> RbResult<Vec<Wrap<AnyValue<'s>>>> {
    let mut val = Vec::with_capacity(arr.len());
    for v in arr.each() {
        val.push(Wrap::<AnyValue<'s>>::try_convert(v?)?);
    }
    Ok(val)
}

impl RbSeries {
    pub fn new_from_any_values(name: String, values: RArray, strict: bool) -> RbResult<Self> {
        let any_values_result = vec_wrap_any_value(values);
        // from anyvalues is fallible
        let result = any_values_result.and_then(|avs| {
            let avs = slice_extract_wrapped(&avs);
            let s = Series::from_any_values(&name, avs, strict).map_err(|e| {
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

    pub fn new_str(name: String, val: Wrap<StringChunked>, _strict: bool) -> Self {
        let mut s = val.0.into_series();
        s.rename(&name);
        RbSeries::new(s)
    }

    pub fn new_binary(name: String, val: Wrap<BinaryChunked>, _strict: bool) -> Self {
        let mut s = val.0.into_series();
        s.rename(&name);
        RbSeries::new(s)
    }

    pub fn new_null(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let s = Series::new_null(&name, val.len());
        Ok(s.into())
    }

    pub fn new_object(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let val = val
            .each()
            .map(|v| v.map(ObjectValue::from))
            .collect::<RbResult<Vec<ObjectValue>>>()?;
        let s = ObjectChunked::<ObjectValue>::new_from_vec(&name, val).into_series();
        Ok(s.into())
    }

    pub fn new_series_list(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let series_vec = to_series_collection(val)?;
        Ok(Series::new(&name, &series_vec).into())
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
        let out = Series::new(&name, &val);
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

    pub fn repeat(
        name: String,
        val: Wrap<AnyValue>,
        n: usize,
        dtype: Wrap<DataType>,
    ) -> RbResult<Self> {
        let av = val.0;
        Ok(Series::new(&name, &[av])
            .cast(&dtype.0)
            .map_err(RbPolarsErr::from)?
            .new_from_index(0, n)
            .into())
    }
}

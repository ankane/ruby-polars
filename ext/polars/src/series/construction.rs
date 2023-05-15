use magnus::{RArray, Value};
use polars_core::prelude::*;

use crate::conversion::{slice_extract_wrapped, Wrap};
use crate::prelude::ObjectValue;
use crate::series::to_series_collection;
use crate::{RbPolarsErr, RbResult, RbSeries};

impl RbSeries {
    pub fn new_opt_bool(name: String, obj: RArray, strict: bool) -> RbResult<RbSeries> {
        let len = obj.len();
        let mut builder = BooleanChunkedBuilder::new(&name, len);

        unsafe {
            for item in obj.as_slice().iter() {
                if item.is_nil() {
                    builder.append_null()
                } else {
                    match item.try_convert::<bool>() {
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
                match item.try_convert::<T::Native>() {
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
        val.push(v?.try_convert()?);
    }
    Ok(val)
}

impl RbSeries {
    pub fn new_from_anyvalues(name: String, val: RArray, strict: bool) -> RbResult<Self> {
        let val = vec_wrap_any_value(val)?;
        let avs = slice_extract_wrapped(&val);
        // from anyvalues is fallible
        let s = Series::from_any_values(&name, avs, strict).map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn new_str(name: String, val: Wrap<Utf8Chunked>, _strict: bool) -> Self {
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

    pub fn new_decimal(name: String, val: RArray, strict: bool) -> RbResult<Self> {
        let val = vec_wrap_any_value(val)?;
        // TODO: do we have to respect 'strict' here? it's possible if we want to
        let avs = slice_extract_wrapped(&val);
        // create a fake dtype with a placeholder "none" scale, to be inferred later
        let dtype = DataType::Decimal(None, None);
        let s = Series::from_any_values_and_dtype(&name, avs, &dtype, strict)
            .map_err(RbPolarsErr::from)?;
        Ok(s.into())
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

    pub fn new_opt_date(name: String, values: RArray, _strict: Option<bool>) -> RbResult<Self> {
        let len = values.len();
        let mut builder = PrimitiveChunkedBuilder::<Int32Type>::new(&name, len);
        for item in values.each() {
            let v = item?;
            if v.is_nil() {
                builder.append_null();
            } else {
                // convert to DateTime for UTC
                let v = v
                    .funcall::<_, _, Value>("to_datetime", ())?
                    .funcall::<_, _, Value>("to_time", ())?
                    .funcall::<_, _, i64>("to_i", ())?;

                // TODO use strict
                builder.append_value((v / 86400) as i32);
            }
        }
        let ca: ChunkedArray<Int32Type> = builder.finish();
        Ok(ca.into_date().into_series().into())
    }

    pub fn new_opt_datetime(name: String, values: RArray, _strict: Option<bool>) -> RbResult<Self> {
        let len = values.len();
        let mut builder = PrimitiveChunkedBuilder::<Int64Type>::new(&name, len);
        for item in values.each() {
            let v = item?;
            if v.is_nil() {
                builder.append_null();
            } else {
                let sec: i64 = v.funcall("to_i", ())?;
                let nsec: i64 = v.funcall("nsec", ())?;
                // TODO use strict
                builder.append_value(sec * 1_000_000_000 + nsec);
            }
        }
        let ca: ChunkedArray<Int64Type> = builder.finish();
        Ok(ca
            .into_datetime(TimeUnit::Nanoseconds, None)
            .into_series()
            .into())
    }
}

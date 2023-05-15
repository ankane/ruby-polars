use magnus::{RArray, Value};
use polars_core::prelude::*;
use polars_core::utils::CustomIterTools;

use crate::conversion::{get_rbseq, slice_extract_wrapped, Wrap};
use crate::prelude::ObjectValue;
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

impl RbSeries {
    pub fn new_from_anyvalues(
        name: String,
        val: RArray,
        strict: bool,
    ) -> RbResult<Self> {
        let mut val2 = Vec::new();
        for v in val.each() {
            val2.push(v?.try_convert()?);
        }
        let avs = slice_extract_wrapped(&val2);
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

    pub fn new_object(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let val = val
            .each()
            .map(|v| v.map(ObjectValue::from))
            .collect::<RbResult<Vec<ObjectValue>>>()?;
        let s = ObjectChunked::<ObjectValue>::new_from_vec(&name, val).into_series();
        Ok(s.into())
    }

    pub fn new_list(name: String, seq: Value, dtype: Wrap<DataType>) -> RbResult<Self> {
        rb_seq_to_list(&name, seq, &dtype.0).map(|s| s.into())
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

pub fn rb_seq_to_list(name: &str, seq: Value, dtype: &DataType) -> RbResult<Series> {
    let (seq, len) = get_rbseq(seq)?;

    let s = match dtype {
        DataType::Int64 => {
            let mut builder =
                ListPrimitiveChunkedBuilder::<Int64Type>::new(name, len, len * 5, DataType::Int64);
            for sub_seq in seq.each() {
                let sub_seq = sub_seq?;
                let (sub_seq, len) = get_rbseq(sub_seq)?;

                // safety: we know the iterators len
                let iter = unsafe {
                    sub_seq
                        .each()
                        .map(|v| {
                            let v = v.unwrap();
                            if v.is_nil() {
                                None
                            } else {
                                Some(v.try_convert::<i64>().unwrap())
                            }
                        })
                        .trust_my_length(len)
                };
                builder.append_iter(iter)
            }
            builder.finish().into_series()
        }
        DataType::Float64 => {
            let mut builder = ListPrimitiveChunkedBuilder::<Float64Type>::new(
                name,
                len,
                len * 5,
                DataType::Float64,
            );
            for sub_seq in seq.each() {
                let sub_seq = sub_seq?;
                let (sub_seq, len) = get_rbseq(sub_seq)?;
                // safety: we know the iterators len
                let iter = unsafe {
                    sub_seq
                        .each()
                        .map(|v| {
                            let v = v.unwrap();
                            if v.is_nil() {
                                None
                            } else {
                                Some(v.try_convert::<f64>().unwrap())
                            }
                        })
                        .trust_my_length(len)
                };
                builder.append_iter(iter)
            }
            builder.finish().into_series()
        }
        DataType::Boolean => {
            let mut builder = ListBooleanChunkedBuilder::new(name, len, len * 5);
            for sub_seq in seq.each() {
                let sub_seq = sub_seq?;
                let (sub_seq, len) = get_rbseq(sub_seq)?;
                // safety: we know the iterators len
                let iter = unsafe {
                    sub_seq
                        .each()
                        .map(|v| {
                            let v = v.unwrap();
                            if v.is_nil() {
                                None
                            } else {
                                Some(v.try_convert::<bool>().unwrap())
                            }
                        })
                        .trust_my_length(len)
                };
                builder.append_iter(iter)
            }
            builder.finish().into_series()
        }
        DataType::Utf8 => {
            return Err(RbPolarsErr::todo());
        }
        dt => {
            return Err(RbPolarsErr::other(format!(
                "cannot create list array from {:?}",
                dt
            )));
        }
    };

    Ok(s)
}

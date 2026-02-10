use magnus::encoding::EncodingCapable;
use magnus::{
    IntoValue, RArray, RHash, RString, Ruby, TryConvert, Value, prelude::*, r_hash::ForEach,
};
use num_traits::ToPrimitive;
use polars::prelude::*;
use polars_compute::decimal::{DEC128_MAX_PREC, DecimalFmtBuffer, dec128_fits};
use polars_core::utils::any_values_to_supertype_and_n_dtypes;

use super::datetime::datetime_to_rb_object;
use super::{ObjectValue, Wrap, struct_dict};

use crate::exceptions::RbOverflowError;
use crate::rb_modules::pl_utils;
use crate::{RbErr, RbPolarsErr, RbResult, RbSeries, RbValueError};

impl IntoValue for Wrap<AnyValue<'_>> {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        any_value_into_rb_object(self.0, ruby)
    }
}

impl TryConvert for Wrap<AnyValue<'_>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        rb_object_to_any_value(ob, true).map(Wrap)
    }
}

pub(crate) fn any_value_into_rb_object(av: AnyValue, ruby: &Ruby) -> Value {
    match av {
        AnyValue::UInt8(v) => ruby.into_value(v),
        AnyValue::UInt16(v) => ruby.into_value(v),
        AnyValue::UInt32(v) => ruby.into_value(v),
        AnyValue::UInt64(v) => ruby.into_value(v),
        AnyValue::UInt128(v) => ruby.into_value(v),
        AnyValue::Int8(v) => ruby.into_value(v),
        AnyValue::Int16(v) => ruby.into_value(v),
        AnyValue::Int32(v) => ruby.into_value(v),
        AnyValue::Int64(v) => ruby.into_value(v),
        AnyValue::Int128(v) => ruby.into_value(v),
        AnyValue::Float16(v) => ruby.into_value(v.to_f32()),
        AnyValue::Float32(v) => ruby.into_value(v),
        AnyValue::Float64(v) => ruby.into_value(v),
        AnyValue::Null => ruby.qnil().as_value(),
        AnyValue::Boolean(v) => ruby.into_value(v),
        AnyValue::String(v) => ruby.into_value(v),
        AnyValue::StringOwned(v) => ruby.into_value(v.as_str()),
        AnyValue::Categorical(cat, map) | AnyValue::Enum(cat, map) => unsafe {
            map.cat_to_str_unchecked(cat).into_value_with(ruby)
        },
        AnyValue::CategoricalOwned(cat, map) | AnyValue::EnumOwned(cat, map) => unsafe {
            map.cat_to_str_unchecked(cat).into_value_with(ruby)
        },
        AnyValue::Date(v) => pl_utils(ruby).funcall("_to_ruby_date", (v,)).unwrap(),
        AnyValue::Datetime(v, time_unit, time_zone) => {
            datetime_to_rb_object(v, time_unit, time_zone)
        }
        AnyValue::DatetimeOwned(v, time_unit, time_zone) => {
            datetime_to_rb_object(v, time_unit, time_zone.as_ref().map(AsRef::as_ref))
        }
        AnyValue::Duration(v, time_unit) => {
            let time_unit = time_unit.to_ascii();
            pl_utils(ruby)
                .funcall("_to_ruby_duration", (v, time_unit))
                .unwrap()
        }
        AnyValue::Time(v) => pl_utils(ruby).funcall("_to_ruby_time", (v,)).unwrap(),
        AnyValue::Array(v, _) | AnyValue::List(v) => RbSeries::new(v).to_a().unwrap().as_value(),
        ref av @ AnyValue::Struct(_, _, flds) => struct_dict(ruby, av._iter_struct_av(), flds),
        AnyValue::StructOwned(payload) => struct_dict(ruby, payload.0.into_iter(), &payload.1),
        AnyValue::Object(v) => {
            let object = v.as_any().downcast_ref::<ObjectValue>().unwrap();
            object.to_value()
        }
        AnyValue::ObjectOwned(v) => {
            let object = v.0.as_any().downcast_ref::<ObjectValue>().unwrap();
            object.to_value()
        }
        AnyValue::Binary(v) => ruby.str_from_slice(v).as_value(),
        AnyValue::BinaryOwned(v) => ruby.str_from_slice(&v).as_value(),
        AnyValue::Decimal(v, prec, scale) => {
            let mut buf = DecimalFmtBuffer::new();
            let s = buf.format_dec128(v, scale, false, false);
            pl_utils(ruby)
                .funcall("_to_ruby_decimal", (prec, s))
                .unwrap()
        }
    }
}

pub(crate) fn rb_object_to_any_value<'s>(ob: Value, strict: bool) -> RbResult<AnyValue<'s>> {
    // Conversion functions.
    fn get_null(_ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        Ok(AnyValue::Null)
    }

    fn get_bool(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        let b = bool::try_convert(ob)?;
        Ok(AnyValue::Boolean(b))
    }

    fn get_int(ob: Value, strict: bool) -> RbResult<AnyValue<'static>> {
        if let Ok(v) = i64::try_convert(ob) {
            Ok(AnyValue::Int64(v))
        } else if let Ok(v) = i128::try_convert(ob) {
            Ok(AnyValue::Int128(v))
        } else if let Ok(v) = u64::try_convert(ob) {
            Ok(AnyValue::UInt64(v))
        } else if let Ok(v) = u128::try_convert(ob) {
            Ok(AnyValue::UInt128(v))
        } else if !strict {
            let f = f64::try_convert(ob)?;
            Ok(AnyValue::Float64(f))
        } else {
            Err(RbOverflowError::new_err(format!(
                "int value too large for Polars integer types: {ob}"
            )))
        }
    }

    fn get_float(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        Ok(AnyValue::Float64(f64::try_convert(ob)?))
    }

    fn get_str(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        let ruby = Ruby::get_with(ob);
        let v = RString::from_value(ob).unwrap();
        if v.enc_get() == ruby.utf8_encindex() {
            Ok(AnyValue::StringOwned(v.to_string()?.into()))
        } else {
            Ok(AnyValue::BinaryOwned(unsafe { v.as_slice() }.to_vec()))
        }
    }

    fn get_list(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        let v = RArray::from_value(ob).unwrap();
        if v.is_empty() {
            Ok(AnyValue::List(Series::new_empty(
                PlSmallStr::EMPTY,
                &DataType::Null,
            )))
        } else {
            let list = v;

            let mut avs = Vec::with_capacity(25);
            let mut iter = list.into_iter();

            for item in (&mut iter).take(25) {
                avs.push(Wrap::<AnyValue>::try_convert(item)?.0)
            }

            let (dtype, _n_types) =
                any_values_to_supertype_and_n_dtypes(&avs).map_err(RbPolarsErr::from)?;

            // push the rest
            avs.reserve(list.len());
            for item in iter {
                avs.push(Wrap::<AnyValue>::try_convert(item)?.0)
            }

            let s = Series::from_any_values_and_dtype(PlSmallStr::EMPTY, &avs, &dtype, true)
                .map_err(RbPolarsErr::from)?;
            Ok(AnyValue::List(s))
        }
    }

    fn get_list_from_series(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        let s = super::get_series(ob)?;
        Ok(AnyValue::List(s))
    }

    fn get_struct(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        let dict = RHash::try_convert(ob)?;
        let len = dict.len();
        let mut keys = Vec::with_capacity(len);
        let mut vals = Vec::with_capacity(len);
        dict.foreach(|key: String, val: Wrap<AnyValue>| {
            let val = val.0;
            let dtype = DataType::from(&val);
            keys.push(Field::new(key.into(), dtype));
            vals.push(val);
            Ok(ForEach::Continue)
        })?;
        Ok(AnyValue::StructOwned(Box::new((vals, keys))))
    }

    fn get_date(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        // convert to DateTime for UTC
        let v = ob
            .funcall::<_, _, Value>("to_datetime", ())?
            .funcall::<_, _, Value>("to_time", ())?
            .funcall::<_, _, i64>("to_i", ())?;
        Ok(AnyValue::Date((v / 86400) as i32))
    }

    fn get_time(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        let sec = ob.funcall::<_, _, i64>("to_i", ())?;
        let nsec = ob.funcall::<_, _, i64>("nsec", ())?;
        let v = sec * 1_000_000_000 + nsec;
        // TODO support time zone when possible
        // https://github.com/pola-rs/polars/issues/9103
        Ok(AnyValue::Datetime(v, TimeUnit::Nanoseconds, None))
    }

    fn get_datetime(ob: Value, strict: bool) -> RbResult<AnyValue<'static>> {
        get_time(ob.funcall("to_time", ())?, strict)
    }

    fn get_decimal(ob: Value, _strict: bool) -> RbResult<AnyValue<'static>> {
        fn abs_decimal_from_digits(digits: String, exp: i32) -> Option<(i128, usize)> {
            let exp = exp - (digits.len() as i32);
            match digits.parse::<i128>() {
                Ok(mut v) => {
                    let scale = if exp > 0 {
                        v = 10_i128.checked_pow(exp as u32)?.checked_mul(v)?;
                        0
                    } else {
                        (-exp) as usize
                    };
                    dec128_fits(v, DEC128_MAX_PREC).then_some((v, scale))
                }
                Err(_) => None,
            }
        }

        let (sign, digits, _, exp): (i8, String, i32, i32) = ob.funcall("split", ()).unwrap();
        let (mut v, scale) = abs_decimal_from_digits(digits, exp).ok_or_else(|| {
            RbErr::from(RbPolarsErr::Other(
                "BigDecimal is too large to fit in Decimal128".into(),
            ))
        })?;
        if sign < 0 {
            // TODO better error
            v = v.checked_neg().unwrap();
        }
        Ok(AnyValue::Decimal(v, DEC128_MAX_PREC, scale))
    }

    let ruby = Ruby::get_with(ob);
    if ob.is_nil() {
        get_null(ob, strict)
    } else if ob.is_kind_of(ruby.class_true_class()) || ob.is_kind_of(ruby.class_false_class()) {
        get_bool(ob, strict)
    } else if ob.is_kind_of(ruby.class_integer()) {
        get_int(ob, strict)
    } else if ob.is_kind_of(ruby.class_float()) {
        get_float(ob, strict)
    } else if ob.is_kind_of(ruby.class_string()) {
        get_str(ob, strict)
    } else if ob.is_kind_of(ruby.class_array()) {
        get_list(ob, strict)
    } else if ob.is_kind_of(ruby.class_hash()) {
        get_struct(ob, strict)
    } else if ob.respond_to("_s", true)? {
        get_list_from_series(ob, strict)
    // call is_a? for ActiveSupport::TimeWithZone
    } else if ob.funcall::<_, _, bool>("is_a?", (ruby.class_time(),))? {
        get_time(ob, strict)
    } else if ob.is_kind_of(crate::rb_modules::datetime(&ruby)) {
        get_datetime(ob, strict)
    } else if ob.is_kind_of(crate::rb_modules::date(&ruby)) {
        get_date(ob, strict)
    } else if ob.is_kind_of(crate::rb_modules::bigdecimal(&ruby)) {
        get_decimal(ob, strict)
    } else {
        Err(RbValueError::new_err(format!("Cannot convert {ob}")))
    }
}

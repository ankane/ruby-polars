use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

use magnus::{
    class, exception, r_hash::ForEach, ruby_handle::RubyHandle, Integer, IntoValue, Module, RArray,
    RFloat, RHash, RString, Symbol, TryConvert, Value, QNIL,
};
use magnus::encoding::{EncodingCapable, Index};
use polars::chunked_array::object::PolarsObjectSafe;
use polars::chunked_array::ops::{FillNullLimit, FillNullStrategy};
use polars::datatypes::AnyValue;
use polars::frame::row::{any_values_to_dtype, Row};
use polars::frame::NullStrategy;
use polars::io::avro::AvroCompression;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use smartstring::alias::String as SmartString;

use crate::object::OBJECT_NAME;
use crate::rb_modules::utils;
use crate::{RbDataFrame, RbLazyFrame, RbPolarsErr, RbResult, RbSeries, RbTypeError, RbValueError};

pub(crate) fn slice_to_wrapped<T>(slice: &[T]) -> &[Wrap<T>] {
    // Safety:
    // Wrap is transparent.
    unsafe { std::mem::transmute(slice) }
}

pub(crate) fn slice_extract_wrapped<T>(slice: &[Wrap<T>]) -> &[T] {
    // Safety:
    // Wrap is transparent.
    unsafe { std::mem::transmute(slice) }
}

pub(crate) fn vec_extract_wrapped<T>(buf: Vec<Wrap<T>>) -> Vec<T> {
    // Safety:
    // Wrap is transparent.
    unsafe { std::mem::transmute(buf) }
}

#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> Clone for Wrap<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Wrap(self.0.clone())
    }
}

impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}

pub(crate) fn get_rbseq(obj: Value) -> RbResult<(RArray, usize)> {
    let seq: RArray = obj.try_convert()?;
    let len = seq.len();
    Ok((seq, len))
}

pub(crate) fn get_df(obj: Value) -> RbResult<DataFrame> {
    let rbdf = obj.funcall::<_, _, &RbDataFrame>("_df", ())?;
    Ok(rbdf.df.borrow().clone())
}

pub(crate) fn get_lf(obj: Value) -> RbResult<LazyFrame> {
    let rbdf = obj.funcall::<_, _, &RbLazyFrame>("_ldf", ())?;
    Ok(rbdf.ldf.clone())
}

pub(crate) fn get_series(obj: Value) -> RbResult<Series> {
    let rbs = obj.funcall::<_, _, &RbSeries>("_s", ())?;
    Ok(rbs.series.borrow().clone())
}

impl TryConvert for Wrap<Utf8Chunked> {
    fn try_convert(obj: Value) -> RbResult<Self> {
        let (seq, len) = get_rbseq(obj)?;
        let mut builder = Utf8ChunkedBuilder::new("", len, len * 25);

        for res in seq.each() {
            let item = res?;
            match item.try_convert::<String>() {
                Ok(val) => builder.append_value(&val),
                Err(_) => builder.append_null(),
            }
        }
        Ok(Wrap(builder.finish()))
    }
}

impl TryConvert for Wrap<BinaryChunked> {
    fn try_convert(obj: Value) -> RbResult<Self> {
        let (seq, len) = get_rbseq(obj)?;
        let mut builder = BinaryChunkedBuilder::new("", len, len * 25);

        for res in seq.each() {
            let item = res?;
            match item.try_convert::<RString>() {
                Ok(val) => builder.append_value(unsafe { val.as_slice() }),
                Err(_) => builder.append_null(),
            }
        }
        Ok(Wrap(builder.finish()))
    }
}

impl TryConvert for Wrap<NullValues> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if let Ok(s) = ob.try_convert::<String>() {
            Ok(Wrap(NullValues::AllColumnsSingle(s)))
        } else if let Ok(s) = ob.try_convert::<Vec<String>>() {
            Ok(Wrap(NullValues::AllColumns(s)))
        } else if let Ok(s) = ob.try_convert::<Vec<(String, String)>>() {
            Ok(Wrap(NullValues::Named(s)))
        } else {
            Err(RbPolarsErr::other(
                "could not extract value from null_values argument".into(),
            ))
        }
    }
}

fn struct_dict<'a>(vals: impl Iterator<Item = AnyValue<'a>>, flds: &[Field]) -> Value {
    let dict = RHash::new();
    for (fld, val) in flds.iter().zip(vals) {
        dict.aset(fld.name().as_str(), Wrap(val)).unwrap()
    }
    dict.into_value()
}

impl IntoValue for Wrap<AnyValue<'_>> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        match self.0 {
            AnyValue::UInt8(v) => Value::from(v),
            AnyValue::UInt16(v) => Value::from(v),
            AnyValue::UInt32(v) => Value::from(v),
            AnyValue::UInt64(v) => Value::from(v),
            AnyValue::Int8(v) => Value::from(v),
            AnyValue::Int16(v) => Value::from(v),
            AnyValue::Int32(v) => Value::from(v),
            AnyValue::Int64(v) => Value::from(v),
            AnyValue::Float32(v) => Value::from(v),
            AnyValue::Float64(v) => Value::from(v),
            AnyValue::Null => *QNIL,
            AnyValue::Boolean(v) => Value::from(v),
            AnyValue::Utf8(v) => Value::from(v),
            AnyValue::Utf8Owned(v) => Value::from(v.as_str()),
            AnyValue::Categorical(_idx, _rev, _arr) => todo!(),
            AnyValue::Date(v) => class::time()
                .funcall::<_, _, Value>("at", (v * 86400,))
                .unwrap()
                .funcall::<_, _, Value>("utc", ())
                .unwrap()
                .funcall::<_, _, Value>("to_date", ())
                .unwrap(),
            AnyValue::Datetime(v, tu, tz) => {
                let t = match tu {
                    TimeUnit::Nanoseconds => {
                        let sec = v / 1000000000;
                        let subsec = v % 1000000000;
                        class::time()
                            .funcall::<_, _, Value>("at", (sec, subsec, Symbol::new("nsec")))
                            .unwrap()
                    }
                    TimeUnit::Microseconds => {
                        let sec = v / 1000000;
                        let subsec = v % 1000000;
                        class::time()
                            .funcall::<_, _, Value>("at", (sec, subsec, Symbol::new("usec")))
                            .unwrap()
                    }
                    TimeUnit::Milliseconds => {
                        let sec = v / 1000;
                        let subsec = v % 1000;
                        class::time()
                            .funcall::<_, _, Value>("at", (sec, subsec, Symbol::new("millisecond")))
                            .unwrap()
                    }
                };

                if tz.is_some() {
                    todo!();
                } else {
                    t.funcall::<_, _, Value>("utc", ()).unwrap()
                }
            }
            AnyValue::Duration(v, tu) => {
                let tu = tu.to_ascii();
                utils().funcall("_to_ruby_duration", (v, tu)).unwrap()
            }
            AnyValue::Time(_v) => todo!(),
            AnyValue::List(v) => RbSeries::new(v).to_a().into_value(),
            ref av @ AnyValue::Struct(_, _, flds) => struct_dict(av._iter_struct_av(), flds),
            AnyValue::StructOwned(payload) => struct_dict(payload.0.into_iter(), &payload.1),
            AnyValue::Object(v) => {
                let object = v.as_any().downcast_ref::<ObjectValue>().unwrap();
                object.inner
            }
            AnyValue::ObjectOwned(v) => {
                let object = v.0.as_any().downcast_ref::<ObjectValue>().unwrap();
                object.inner
            }
            AnyValue::Binary(v) => RString::from_slice(v).into_value(),
            AnyValue::BinaryOwned(v) => RString::from_slice(&v).into_value(),
            AnyValue::Decimal(_v, _scale) => todo!(),
        }
    }
}

impl IntoValue for Wrap<DataType> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let pl = crate::rb_modules::polars();

        match self.0 {
            DataType::Int8 => pl.const_get::<_, Value>("Int8").unwrap(),
            DataType::Int16 => pl.const_get::<_, Value>("Int16").unwrap(),
            DataType::Int32 => pl.const_get::<_, Value>("Int32").unwrap(),
            DataType::Int64 => pl.const_get::<_, Value>("Int64").unwrap(),
            DataType::UInt8 => pl.const_get::<_, Value>("UInt8").unwrap(),
            DataType::UInt16 => pl.const_get::<_, Value>("UInt16").unwrap(),
            DataType::UInt32 => pl.const_get::<_, Value>("UInt32").unwrap(),
            DataType::UInt64 => pl.const_get::<_, Value>("UInt64").unwrap(),
            DataType::Float32 => pl.const_get::<_, Value>("Float32").unwrap(),
            DataType::Float64 => pl.const_get::<_, Value>("Float64").unwrap(),
            DataType::Decimal(_precision, _scale) => todo!(),
            DataType::Boolean => pl.const_get::<_, Value>("Boolean").unwrap(),
            DataType::Utf8 => pl.const_get::<_, Value>("Utf8").unwrap(),
            DataType::Binary => pl.const_get::<_, Value>("Binary").unwrap(),
            DataType::List(inner) => {
                let inner = Wrap(*inner);
                let list_class = pl.const_get::<_, Value>("List").unwrap();
                list_class.funcall::<_, _, Value>("new", (inner,)).unwrap()
            }
            DataType::Date => pl.const_get::<_, Value>("Date").unwrap(),
            DataType::Datetime(tu, tz) => {
                let datetime_class = pl.const_get::<_, Value>("Datetime").unwrap();
                datetime_class
                    .funcall::<_, _, Value>("new", (tu.to_ascii(), tz))
                    .unwrap()
            }
            DataType::Duration(tu) => {
                let duration_class = pl.const_get::<_, Value>("Duration").unwrap();
                duration_class
                    .funcall::<_, _, Value>("new", (tu.to_ascii(),))
                    .unwrap()
            }
            DataType::Object(_) => pl.const_get::<_, Value>("Object").unwrap(),
            DataType::Categorical(_) => pl.const_get::<_, Value>("Categorical").unwrap(),
            DataType::Time => pl.const_get::<_, Value>("Time").unwrap(),
            DataType::Struct(fields) => {
                let field_class = pl.const_get::<_, Value>("Field").unwrap();
                let iter = fields.iter().map(|fld| {
                    let name = fld.name().as_str();
                    let dtype = Wrap(fld.data_type().clone());
                    field_class
                        .funcall::<_, _, Value>("new", (name, dtype))
                        .unwrap()
                });
                let fields = RArray::from_iter(iter);
                let struct_class = pl.const_get::<_, Value>("Struct").unwrap();
                struct_class
                    .funcall::<_, _, Value>("new", (fields,))
                    .unwrap()
            }
            DataType::Null => pl.const_get::<_, Value>("Null").unwrap(),
            DataType::Unknown => pl.const_get::<_, Value>("Unknown").unwrap(),
        }
    }
}

impl IntoValue for Wrap<TimeUnit> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let tu = match self.0 {
            TimeUnit::Nanoseconds => "ns",
            TimeUnit::Microseconds => "us",
            TimeUnit::Milliseconds => "ms",
        };
        tu.into_value()
    }
}

impl IntoValue for Wrap<&Utf8Chunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let iter = self.0.into_iter();
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&BinaryChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let iter = self
            .0
            .into_iter()
            .map(|opt_bytes| opt_bytes.map(RString::from_slice));
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&StructChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let s = self.0.clone().into_series();
        // todo! iterate its chunks and flatten.
        // make series::iter() accept a chunk index.
        let s = s.rechunk();
        let iter = s.iter().map(|av| {
            if let AnyValue::Struct(_, _, flds) = av {
                struct_dict(av._iter_struct_av(), flds)
            } else {
                unreachable!()
            }
        });

        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DurationChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let utils = utils();
        let time_unit = Wrap(self.0.time_unit()).into_value();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_duration", (v, time_unit))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DatetimeChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let utils = utils();
        let time_unit = Wrap(self.0.time_unit()).into_value();
        let time_zone = self.0.time_zone().clone().into_value();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_datetime", (v, time_unit, time_zone))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&TimeChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let utils = utils();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_time", (v,))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DateChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        let utils = utils();
        let iter = self.0.into_iter().map(|opt_v| {
            opt_v.map(|v| {
                utils
                    .funcall::<_, _, Value>("_to_ruby_date", (v,))
                    .unwrap()
            })
        });
        RArray::from_iter(iter).into_value()
    }
}

impl IntoValue for Wrap<&DecimalChunked> {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        todo!();
    }
}

impl TryConvert for Wrap<Field> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let name: String = ob.funcall("name", ())?;
        let dtype: Wrap<DataType> = ob.funcall("dtype", ())?;
        Ok(Wrap(Field::new(&name, dtype.0)))
    }
}

impl TryConvert for Wrap<DataType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let dtype = if ob.is_kind_of(class::class()) {
            let name = ob.funcall::<_, _, String>("name", ())?;
            match name.as_str() {
                "Polars::UInt8" => DataType::UInt8,
                "Polars::UInt16" => DataType::UInt16,
                "Polars::UInt32" => DataType::UInt32,
                "Polars::UInt64" => DataType::UInt64,
                "Polars::Int8" => DataType::Int8,
                "Polars::Int16" => DataType::Int16,
                "Polars::Int32" => DataType::Int32,
                "Polars::Int64" => DataType::Int64,
                "Polars::Utf8" => DataType::Utf8,
                "Polars::Binary" => DataType::Binary,
                "Polars::Boolean" => DataType::Boolean,
                "Polars::Categorical" => DataType::Categorical(None),
                "Polars::Date" => DataType::Date,
                "Polars::Datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
                "Polars::Time" => DataType::Time,
                "Polars::Duration" => DataType::Duration(TimeUnit::Microseconds),
                "Polars::Decimal" => DataType::Decimal(None, None),
                "Polars::Float32" => DataType::Float32,
                "Polars::Float64" => DataType::Float64,
                "Polars::Object" => DataType::Object(OBJECT_NAME),
                // TODO change to Null
                "Polars::List" => DataType::List(Box::new(DataType::Boolean)),
                "Polars::Null" => DataType::Null,
                "Polars::Unknown" => DataType::Unknown,
                dt => {
                    return Err(RbValueError::new_err(format!(
                        "{dt} is not a correct polars DataType.",
                    )))
                }
            }
        // TODO improve
        } else if ob.try_convert::<String>().is_err() {
            let name = unsafe { ob.class().name() }.into_owned();
            match name.as_str() {
                "Polars::Duration" => {
                    let time_unit: Value = ob.funcall("time_unit", ()).unwrap();
                    let time_unit = time_unit.try_convert::<Wrap<TimeUnit>>()?.0;
                    DataType::Duration(time_unit)
                }
                "Polars::Datetime" => {
                    let time_unit: Value = ob.funcall("time_unit", ()).unwrap();
                    let time_unit = time_unit.try_convert::<Wrap<TimeUnit>>()?.0;
                    let time_zone: Value = ob.funcall("time_zone", ()).unwrap();
                    let time_zone = time_zone.try_convert()?;
                    DataType::Datetime(time_unit, time_zone)
                }
                "Polars::Decimal" => {
                    let precision = ob.funcall::<_, _, Value>("precision", ())?.try_convert()?;
                    let scale = ob.funcall::<_, _, Value>("scale", ())?.try_convert()?;
                    DataType::Decimal(precision, Some(scale))
                }
                "Polars::List" => {
                    let inner: Value = ob.funcall("inner", ()).unwrap();
                    let inner = inner.try_convert::<Wrap<DataType>>()?;
                    DataType::List(Box::new(inner.0))
                }
                "Polars::Struct" => {
                    let arr: RArray = ob.funcall("fields", ())?;
                    let mut fields = Vec::with_capacity(arr.len());
                    for v in arr.each() {
                        fields.push(v?.try_convert::<Wrap<Field>>()?.0);
                    }
                    DataType::Struct(fields)
                }
                dt => {
                    return Err(RbTypeError::new_err(format!(
                        "A {dt} object is not a correct polars DataType. \
                        Hint: use the class without instantiating it.",
                    )))
                }
            }
        } else {
            match ob.try_convert::<String>()?.as_str() {
                "u8" => DataType::UInt8,
                "u16" => DataType::UInt16,
                "u32" => DataType::UInt32,
                "u64" => DataType::UInt64,
                "i8" => DataType::Int8,
                "i16" => DataType::Int16,
                "i32" => DataType::Int32,
                "i64" => DataType::Int64,
                "str" => DataType::Utf8,
                "bin" => DataType::Binary,
                "bool" => DataType::Boolean,
                "cat" => DataType::Categorical(None),
                "date" => DataType::Date,
                "datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
                "f32" => DataType::Float32,
                "time" => DataType::Time,
                "dur" => DataType::Duration(TimeUnit::Microseconds),
                "f64" => DataType::Float64,
                // "obj" => DataType::Object(OBJECT_NAME),
                "list" => DataType::List(Box::new(DataType::Boolean)),
                "null" => DataType::Null,
                "unk" => DataType::Unknown,
                _ => {
                    return Err(RbValueError::new_err(format!(
                        "{} is not a supported DataType.",
                        ob
                    )))
                }
            }
        };
        Ok(Wrap(dtype))
    }
}

impl<'s> TryConvert for Wrap<AnyValue<'s>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if ob.is_kind_of(class::true_class()) || ob.is_kind_of(class::false_class()) {
            Ok(AnyValue::Boolean(ob.try_convert::<bool>()?).into())
        } else if let Some(v) = Integer::from_value(ob) {
            Ok(AnyValue::Int64(v.to_i64()?).into())
        } else if let Some(v) = RFloat::from_value(ob) {
            Ok(AnyValue::Float64(v.to_f64()).into())
        } else if let Some(v) = RString::from_value(ob) {
            if v.enc_get() == Index::utf8() {
                Ok(AnyValue::Utf8Owned(v.to_string()?.into()).into())
            } else {
                Ok(AnyValue::BinaryOwned(unsafe { v.as_slice() }.to_vec()).into())
            }
        // call is_a? for ActiveSupport::TimeWithZone
        } else if ob.funcall::<_, _, bool>("is_a?", (class::time(),))? {
            let sec = ob.funcall::<_, _, i64>("to_i", ())?;
            let nsec = ob.funcall::<_, _, i64>("nsec", ())?;
            let v = sec * 1_000_000_000 + nsec;
            // TODO support time zone
            Ok(AnyValue::Datetime(v, TimeUnit::Nanoseconds, &None).into())
        } else if ob.is_nil() {
            Ok(AnyValue::Null.into())
        } else if let Some(dict) = RHash::from_value(ob) {
            let len = dict.len();
            let mut keys = Vec::with_capacity(len);
            let mut vals = Vec::with_capacity(len);
            dict.foreach(|k: Value, v: Value| {
                let key = k.try_convert::<String>()?;
                let val = v.try_convert::<Wrap<AnyValue>>()?.0;
                let dtype = DataType::from(&val);
                keys.push(Field::new(&key, dtype));
                vals.push(val);
                Ok(ForEach::Continue)
            })?;
            Ok(Wrap(AnyValue::StructOwned(Box::new((vals, keys)))))
        } else if let Some(v) = RArray::from_value(ob) {
            if v.is_empty() {
                Ok(Wrap(AnyValue::List(Series::new_empty("", &DataType::Null))))
            } else {
                let avs = v.try_convert::<Wrap<Row>>()?.0 .0;
                // use first `n` values to infer datatype
                // this value is not too large as this will be done with every
                // anyvalue that has to be converted, which can be many
                let n = 25;
                let dtype = any_values_to_dtype(&avs[..std::cmp::min(avs.len(), n)])
                    .map_err(RbPolarsErr::from)?;
                let s = Series::from_any_values_and_dtype("", &avs, &dtype, true)
                    .map_err(RbPolarsErr::from)?;
                Ok(Wrap(AnyValue::List(s)))
            }
        } else if ob.is_kind_of(crate::rb_modules::datetime()) {
            let sec: i64 = ob.funcall("to_i", ())?;
            let nsec: i64 = ob.funcall("nsec", ())?;
            Ok(Wrap(AnyValue::Datetime(sec * 1_000_000_000 + nsec, TimeUnit::Nanoseconds, &None)))
        } else if ob.is_kind_of(crate::rb_modules::date()) {
            // convert to DateTime for UTC
            let v = ob
                .funcall::<_, _, Value>("to_datetime", ())?
                .funcall::<_, _, Value>("to_time", ())?
                .funcall::<_, _, i64>("to_i", ())?;
            Ok(Wrap(AnyValue::Date((v / 86400) as i32)))
        } else if ob.is_kind_of(crate::rb_modules::bigdecimal()) {
            todo!();
        } else {
            Err(RbPolarsErr::other(format!(
                "object type not supported {:?}",
                ob
            )))
        }
    }
}

impl<'s> TryConvert for Wrap<Row<'s>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let mut vals: Vec<Wrap<AnyValue<'s>>> = Vec::new();
        for item in ob.try_convert::<RArray>()?.each() {
            vals.push(item?.try_convert::<Wrap<AnyValue<'s>>>()?);
        }
        let vals: Vec<AnyValue> = unsafe { std::mem::transmute(vals) };
        Ok(Wrap(Row(vals)))
    }
}

impl TryConvert for Wrap<Schema> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let dict = ob.try_convert::<RHash>()?;

        let mut schema = Vec::new();
        dict.foreach(|key: String, val: Wrap<DataType>| {
            schema.push(Ok(Field::new(&key, val.0)));
            Ok(ForEach::Continue)
        })
        .unwrap();

        Ok(Wrap(schema.into_iter().collect::<RbResult<Schema>>()?))
    }
}

#[derive(Clone, Debug)]
pub struct ObjectValue {
    pub inner: Value,
}

impl Hash for ObjectValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let h = self
            .inner
            .funcall::<_, _, isize>("hash", ())
            .expect("should be hashable");
        state.write_isize(h)
    }
}

impl Eq for ObjectValue {}

impl PartialEq for ObjectValue {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eql(&other.inner).unwrap_or(false)
    }
}

impl Display for ObjectValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl PolarsObject for ObjectValue {
    fn type_name() -> &'static str {
        "object"
    }
}

impl From<Value> for ObjectValue {
    fn from(v: Value) -> Self {
        Self { inner: v }
    }
}

impl TryConvert for ObjectValue {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(ObjectValue { inner: ob })
    }
}

impl From<&dyn PolarsObjectSafe> for &ObjectValue {
    fn from(val: &dyn PolarsObjectSafe) -> Self {
        unsafe { &*(val as *const dyn PolarsObjectSafe as *const ObjectValue) }
    }
}

// TODO remove
impl ObjectValue {
    pub fn to_object(&self) -> Value {
        self.inner
    }
}

impl IntoValue for ObjectValue {
    fn into_value_with(self, _: &RubyHandle) -> Value {
        self.inner
    }
}

impl Default for ObjectValue {
    fn default() -> Self {
        ObjectValue { inner: *QNIL }
    }
}

pub(crate) fn dicts_to_rows(
    records: &Value,
    infer_schema_len: usize,
) -> RbResult<(Vec<Row>, Vec<String>)> {
    let (dicts, len) = get_rbseq(*records)?;

    let mut key_names = PlIndexSet::new();
    for d in dicts.each().take(infer_schema_len) {
        let d = d?;
        let d = d.try_convert::<RHash>()?;

        d.foreach(|name: Value, _value: Value| {
            if let Some(v) = Symbol::from_value(name) {
                key_names.insert(v.name()?.into());
            } else {
                key_names.insert(name.try_convert::<String>()?);
            };
            Ok(ForEach::Continue)
        })?;
    }

    let mut rows = Vec::with_capacity(len);

    for d in dicts.each() {
        let d = d?;
        let d = d.try_convert::<RHash>()?;

        let mut row = Vec::with_capacity(key_names.len());

        for k in key_names.iter() {
            // TODO improve performance
            let val = match d.get(k.clone()).or_else(|| d.get(Symbol::new(k))) {
                None => AnyValue::Null,
                Some(val) => val.try_convert::<Wrap<AnyValue>>()?.0,
            };
            row.push(val)
        }
        rows.push(Row(row))
    }
    Ok((rows, key_names.into_iter().collect()))
}

impl TryConvert for Wrap<AsofStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "backward" => AsofStrategy::Backward,
            "forward" => AsofStrategy::Forward,
            v => {
                return Err(RbValueError::new_err(format!(
                    "strategy must be one of {{'backward', 'forward'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<InterpolationMethod> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "linear" => InterpolationMethod::Linear,
            "nearest" => InterpolationMethod::Nearest,
            v => {
                return Err(RbValueError::new_err(format!(
                    "method must be one of {{'linear', 'nearest'}}, got {v}",
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<Option<AvroCompression>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "uncompressed" => None,
            "snappy" => Some(AvroCompression::Snappy),
            "deflate" => Some(AvroCompression::Deflate),
            v => {
                return Err(RbValueError::new_err(format!(
                    "compression must be one of {{'uncompressed', 'snappy', 'deflate'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<CategoricalOrdering> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "physical" => CategoricalOrdering::Physical,
            "lexical" => CategoricalOrdering::Lexical,
            v => {
                return Err(RbValueError::new_err(format!(
                    "ordering must be one of {{'physical', 'lexical'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<StartBy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "window" => StartBy::WindowBound,
            "datapoint" => StartBy::DataPoint,
            "monday" => StartBy::Monday,
            v => {
                return Err(RbValueError::new_err(format!(
                    "closed must be one of {{'window', 'datapoint', 'monday'}}, got {v}",
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<ClosedWindow> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "left" => ClosedWindow::Left,
            "right" => ClosedWindow::Right,
            "both" => ClosedWindow::Both,
            "none" => ClosedWindow::None,
            v => {
                return Err(RbValueError::new_err(format!(
                    "closed must be one of {{'left', 'right', 'both', 'none'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<CsvEncoding> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "utf8" => CsvEncoding::Utf8,
            "utf8-lossy" => CsvEncoding::LossyUtf8,
            v => {
                return Err(RbValueError::new_err(format!(
                    "encoding must be one of {{'utf8', 'utf8-lossy'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<Option<IpcCompression>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "uncompressed" => None,
            "lz4" => Some(IpcCompression::LZ4),
            "zstd" => Some(IpcCompression::ZSTD),
            v => {
                return Err(RbValueError::new_err(format!(
                    "compression must be one of {{'uncompressed', 'lz4', 'zstd'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<JoinType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "outer" => JoinType::Outer,
            "semi" => JoinType::Semi,
            "anti" => JoinType::Anti,
            // #[cfg(feature = "cross_join")]
            // "cross" => JoinType::Cross,
            v => {
                return Err(RbValueError::new_err(format!(
                "how must be one of {{'inner', 'left', 'outer', 'semi', 'anti', 'cross'}}, got {}",
                v
            )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<ListToStructWidthStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "first_non_null" => ListToStructWidthStrategy::FirstNonNull,
            "max_width" => ListToStructWidthStrategy::MaxWidth,
            v => {
                return Err(RbValueError::new_err(format!(
                    "n_field_strategy must be one of {{'first_non_null', 'max_width'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<NullBehavior> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "drop" => NullBehavior::Drop,
            "ignore" => NullBehavior::Ignore,
            v => {
                return Err(RbValueError::new_err(format!(
                    "null behavior must be one of {{'drop', 'ignore'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<NullStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "ignore" => NullStrategy::Ignore,
            "propagate" => NullStrategy::Propagate,
            v => {
                return Err(RbValueError::new_err(format!(
                    "null strategy must be one of {{'ignore', 'propagate'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<ParallelStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "auto" => ParallelStrategy::Auto,
            "columns" => ParallelStrategy::Columns,
            "row_groups" => ParallelStrategy::RowGroups,
            "none" => ParallelStrategy::None,
            v => {
                return Err(RbValueError::new_err(format!(
                    "parallel must be one of {{'auto', 'columns', 'row_groups', 'none'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<QuantileInterpolOptions> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "lower" => QuantileInterpolOptions::Lower,
            "higher" => QuantileInterpolOptions::Higher,
            "nearest" => QuantileInterpolOptions::Nearest,
            "linear" => QuantileInterpolOptions::Linear,
            "midpoint" => QuantileInterpolOptions::Midpoint,
            v => {
                return Err(RbValueError::new_err(format!(
                    "interpolation must be one of {{'lower', 'higher', 'nearest', 'linear', 'midpoint'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<RankMethod> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "min" => RankMethod::Min,
            "max" => RankMethod::Max,
            "average" => RankMethod::Average,
            "dense" => RankMethod::Dense,
            "ordinal" => RankMethod::Ordinal,
            "random" => RankMethod::Random,
            v => {
                return Err(RbValueError::new_err(format!(
                    "method must be one of {{'min', 'max', 'average', 'dense', 'ordinal', 'random'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<TimeUnit> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "ns" => TimeUnit::Nanoseconds,
            "us" => TimeUnit::Microseconds,
            "ms" => TimeUnit::Milliseconds,
            v => {
                return Err(RbValueError::new_err(format!(
                    "time unit must be one of {{'ns', 'us', 'ms'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<UniqueKeepStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "first" => UniqueKeepStrategy::First,
            "last" => UniqueKeepStrategy::Last,
            v => {
                return Err(RbValueError::new_err(format!(
                    "keep must be one of {{'first', 'last'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<SearchSortedSide> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match ob.try_convert::<String>()?.as_str() {
            "any" => SearchSortedSide::Any,
            "left" => SearchSortedSide::Left,
            "right" => SearchSortedSide::Right,
            v => {
                return Err(RbValueError::new_err(format!(
                    "side must be one of {{'any', 'left', 'right'}}, got {v}",
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

pub fn parse_fill_null_strategy(
    strategy: &str,
    limit: FillNullLimit,
) -> RbResult<FillNullStrategy> {
    let parsed = match strategy {
        "forward" => FillNullStrategy::Forward(limit),
        "backward" => FillNullStrategy::Backward(limit),
        "min" => FillNullStrategy::Min,
        "max" => FillNullStrategy::Max,
        "mean" => FillNullStrategy::Mean,
        "zero" => FillNullStrategy::Zero,
        "one" => FillNullStrategy::One,
        e => {
            return Err(magnus::Error::new(exception::runtime_error(), format!(
                "strategy must be one of {{'forward', 'backward', 'min', 'max', 'mean', 'zero', 'one'}}, got {}",
                e,
            )))
        }
    };
    Ok(parsed)
}

pub fn parse_parquet_compression(
    compression: &str,
    compression_level: Option<i32>,
) -> RbResult<ParquetCompression> {
    let parsed = match compression {
        "uncompressed" => ParquetCompression::Uncompressed,
        "snappy" => ParquetCompression::Snappy,
        "gzip" => ParquetCompression::Gzip(
            compression_level
                .map(|lvl| {
                    GzipLevel::try_new(lvl as u8)
                        .map_err(|e| RbValueError::new_err(format!("{:?}", e)))
                })
                .transpose()?,
        ),
        "lzo" => ParquetCompression::Lzo,
        "brotli" => ParquetCompression::Brotli(
            compression_level
                .map(|lvl| {
                    BrotliLevel::try_new(lvl as u32)
                        .map_err(|e| RbValueError::new_err(format!("{:?}", e)))
                })
                .transpose()?,
        ),
        "lz4" => ParquetCompression::Lz4Raw,
        "zstd" => ParquetCompression::Zstd(
            compression_level
                .map(|lvl| {
                    ZstdLevel::try_new(lvl)
                        .map_err(|e| RbValueError::new_err(format!("{:?}", e)))
                })
                .transpose()?,
        ),
        e => {
            return Err(RbValueError::new_err(format!(
                "compression must be one of {{'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'zstd'}}, got {}",
                e
            )))
        }
    };
    Ok(parsed)
}

pub(crate) fn strings_to_smartstrings<I, S>(container: I) -> Vec<SmartString>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    container.into_iter().map(|s| s.as_ref().into()).collect()
}

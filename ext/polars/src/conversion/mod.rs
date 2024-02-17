pub(crate) mod anyvalue;
mod chunked_array;

use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use magnus::{
    class, exception, prelude::*, r_hash::ForEach, value::Opaque, IntoValue, Module, RArray, RHash,
    Ruby, Symbol, TryConvert, Value,
};
use polars::chunked_array::object::PolarsObjectSafe;
use polars::chunked_array::ops::{FillNullLimit, FillNullStrategy};
use polars::datatypes::AnyValue;
use polars::frame::row::Row;
use polars::frame::NullStrategy;
use polars::io::avro::AvroCompression;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_utils::total_ord::TotalEq;
use smartstring::alias::String as SmartString;

use crate::object::OBJECT_NAME;
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
    let seq = RArray::try_convert(obj)?;
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

impl TryConvert for Wrap<NullValues> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if let Ok(s) = String::try_convert(ob) {
            Ok(Wrap(NullValues::AllColumnsSingle(s)))
        } else if let Ok(s) = Vec::<String>::try_convert(ob) {
            Ok(Wrap(NullValues::AllColumns(s)))
        } else if let Ok(s) = Vec::<(String, String)>::try_convert(ob) {
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

impl IntoValue for Wrap<DataType> {
    fn into_value_with(self, _: &Ruby) -> Value {
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
            DataType::Decimal(precision, scale) => {
                let decimal_class = pl.const_get::<_, Value>("Decimal").unwrap();
                decimal_class
                    .funcall::<_, _, Value>("new", (precision, scale))
                    .unwrap()
            }
            DataType::Boolean => pl.const_get::<_, Value>("Boolean").unwrap(),
            DataType::String => pl.const_get::<_, Value>("String").unwrap(),
            DataType::Binary => pl.const_get::<_, Value>("Binary").unwrap(),
            DataType::Array(inner, size) => {
                let inner = Wrap(*inner);
                let list_class = pl.const_get::<_, Value>("Array").unwrap();
                list_class
                    .funcall::<_, _, Value>("new", (size, inner))
                    .unwrap()
            }
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
            DataType::Object(_, _) => pl.const_get::<_, Value>("Object").unwrap(),
            DataType::Categorical(_, _) => pl.const_get::<_, Value>("Categorical").unwrap(),
            DataType::Enum(_, _) => {
                todo!()
            }
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
            DataType::BinaryOffset => {
                unimplemented!()
            }
        }
    }
}

impl IntoValue for Wrap<TimeUnit> {
    fn into_value_with(self, _: &Ruby) -> Value {
        let tu = match self.0 {
            TimeUnit::Nanoseconds => "ns",
            TimeUnit::Microseconds => "us",
            TimeUnit::Milliseconds => "ms",
        };
        tu.into_value()
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
                "Polars::String" => DataType::String,
                "Polars::Binary" => DataType::Binary,
                "Polars::Boolean" => DataType::Boolean,
                "Polars::Categorical" => DataType::Categorical(None, Default::default()),
                "Polars::Date" => DataType::Date,
                "Polars::Datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
                "Polars::Time" => DataType::Time,
                "Polars::Duration" => DataType::Duration(TimeUnit::Microseconds),
                "Polars::Decimal" => DataType::Decimal(None, None),
                "Polars::Float32" => DataType::Float32,
                "Polars::Float64" => DataType::Float64,
                "Polars::Object" => DataType::Object(OBJECT_NAME, None),
                "Polars::List" => DataType::List(Box::new(DataType::Null)),
                "Polars::Null" => DataType::Null,
                "Polars::Unknown" => DataType::Unknown,
                dt => {
                    return Err(RbValueError::new_err(format!(
                        "{dt} is not a correct polars DataType.",
                    )))
                }
            }
        // TODO improve
        } else if String::try_convert(ob).is_err() {
            let name = unsafe { ob.class().name() }.into_owned();
            match name.as_str() {
                "Polars::Duration" => {
                    let time_unit: Value = ob.funcall("time_unit", ()).unwrap();
                    let time_unit = Wrap::<TimeUnit>::try_convert(time_unit)?.0;
                    DataType::Duration(time_unit)
                }
                "Polars::Datetime" => {
                    let time_unit: Value = ob.funcall("time_unit", ()).unwrap();
                    let time_unit = Wrap::<TimeUnit>::try_convert(time_unit)?.0;
                    let time_zone = ob.funcall("time_zone", ())?;
                    DataType::Datetime(time_unit, time_zone)
                }
                "Polars::Decimal" => {
                    let precision = ob.funcall("precision", ())?;
                    let scale = ob.funcall("scale", ())?;
                    DataType::Decimal(precision, Some(scale))
                }
                "Polars::List" => {
                    let inner: Value = ob.funcall("inner", ()).unwrap();
                    let inner = Wrap::<DataType>::try_convert(inner)?;
                    DataType::List(Box::new(inner.0))
                }
                "Polars::Struct" => {
                    let arr: RArray = ob.funcall("fields", ())?;
                    let mut fields = Vec::with_capacity(arr.len());
                    for v in arr.each() {
                        fields.push(Wrap::<Field>::try_convert(v?)?.0);
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
            match String::try_convert(ob)?.as_str() {
                "u8" => DataType::UInt8,
                "u16" => DataType::UInt16,
                "u32" => DataType::UInt32,
                "u64" => DataType::UInt64,
                "i8" => DataType::Int8,
                "i16" => DataType::Int16,
                "i32" => DataType::Int32,
                "i64" => DataType::Int64,
                "str" => DataType::String,
                "bin" => DataType::Binary,
                "bool" => DataType::Boolean,
                "cat" => DataType::Categorical(None, Default::default()),
                "date" => DataType::Date,
                "datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
                "f32" => DataType::Float32,
                "time" => DataType::Time,
                "dur" => DataType::Duration(TimeUnit::Microseconds),
                "f64" => DataType::Float64,
                "obj" => DataType::Object(OBJECT_NAME, None),
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

impl<'s> TryConvert for Wrap<Row<'s>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let mut vals: Vec<Wrap<AnyValue<'s>>> = Vec::new();
        for item in RArray::try_convert(ob)?.each() {
            vals.push(Wrap::<AnyValue<'s>>::try_convert(item?)?);
        }
        let vals: Vec<AnyValue> = unsafe { std::mem::transmute(vals) };
        Ok(Wrap(Row(vals)))
    }
}

impl TryConvert for Wrap<Schema> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let dict = RHash::try_convert(ob)?;

        let mut schema = Vec::new();
        dict.foreach(|key: String, val: Wrap<DataType>| {
            schema.push(Ok(Field::new(&key, val.0)));
            Ok(ForEach::Continue)
        })
        .unwrap();

        Ok(Wrap(schema.into_iter().collect::<RbResult<Schema>>()?))
    }
}

#[derive(Clone)]
pub struct ObjectValue {
    pub inner: Opaque<Value>,
}

impl Debug for ObjectValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectValue")
            .field("inner", &self.to_object())
            .finish()
    }
}

impl Hash for ObjectValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let h = self
            .to_object()
            .funcall::<_, _, isize>("hash", ())
            .expect("should be hashable");
        state.write_isize(h)
    }
}

impl Eq for ObjectValue {}

impl PartialEq for ObjectValue {
    fn eq(&self, other: &Self) -> bool {
        self.to_object().eql(other.to_object()).unwrap_or(false)
    }
}

impl TotalEq for ObjectValue {
    fn tot_eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl Display for ObjectValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_object())
    }
}

impl PolarsObject for ObjectValue {
    fn type_name() -> &'static str {
        "object"
    }
}

impl From<Value> for ObjectValue {
    fn from(v: Value) -> Self {
        Self { inner: v.into() }
    }
}

impl TryConvert for ObjectValue {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(ObjectValue { inner: ob.into() })
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
        Ruby::get().unwrap().get_inner(self.inner)
    }
}

impl IntoValue for ObjectValue {
    fn into_value_with(self, _: &Ruby) -> Value {
        self.to_object()
    }
}

impl Default for ObjectValue {
    fn default() -> Self {
        ObjectValue {
            inner: Ruby::get().unwrap().qnil().as_value().into(),
        }
    }
}

pub(crate) fn dicts_to_rows(
    records: &Value,
    infer_schema_len: Option<usize>,
    schema_columns: PlIndexSet<String>,
) -> RbResult<(Vec<Row>, Vec<String>)> {
    let infer_schema_len = infer_schema_len.map(|n| std::cmp::max(1, n));
    let (dicts, len) = get_rbseq(*records)?;

    let key_names = {
        if !schema_columns.is_empty() {
            schema_columns
        } else {
            let mut inferred_keys = PlIndexSet::new();
            for d in dicts.each().take(infer_schema_len.unwrap_or(usize::MAX)) {
                let d = d?;
                let d = RHash::try_convert(d)?;

                d.foreach(|name: Value, _value: Value| {
                    if let Some(v) = Symbol::from_value(name) {
                        inferred_keys.insert(v.name()?.into());
                    } else {
                        inferred_keys.insert(String::try_convert(name)?);
                    };
                    Ok(ForEach::Continue)
                })?;
            }
            inferred_keys
        }
    };

    let mut rows = Vec::with_capacity(len);

    for d in dicts.each() {
        let d = d?;
        let d = RHash::try_convert(d)?;

        let mut row = Vec::with_capacity(key_names.len());

        for k in key_names.iter() {
            // TODO improve performance
            let val = match d.get(k.clone()).or_else(|| d.get(Symbol::new(k))) {
                None => AnyValue::Null,
                Some(val) => Wrap::<AnyValue>::try_convert(val)?.0,
            };
            row.push(val)
        }
        rows.push(Row(row))
    }
    Ok((rows, key_names.into_iter().collect()))
}

impl TryConvert for Wrap<AsofStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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

impl TryConvert for Wrap<IpcCompression> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "lz4" => IpcCompression::LZ4,
            "zstd" => IpcCompression::ZSTD,
            v => {
                return Err(RbValueError::new_err(format!(
                    "compression must be one of {{'lz4', 'zstd'}}, got {}",
                    v
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<QuoteStyle> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "always" => QuoteStyle::Always,
            "necessary" => QuoteStyle::Necessary,
            "non_numeric" => QuoteStyle::NonNumeric,
            "never" => QuoteStyle::Never,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`quote_style` must be one of {{'always', 'necessary', 'non_numeric', 'never'}}, got {v}",
                )))
            },
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<JoinType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "outer" => JoinType::Outer { coalesce: false },
            "outer_coalesce" => JoinType::Outer { coalesce: true },
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

impl TryConvert for Wrap<Label> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "left" => Label::Left,
            "right" => Label::Right,
            "datapoint" => Label::DataPoint,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`label` must be one of {{'left', 'right', 'datapoint'}}, got {v}",
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<ListToStructWidthStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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
        let parsed = match String::try_convert(ob)?.as_str() {
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

impl TryConvert for Wrap<NonZeroUsize> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let v = usize::try_convert(ob)?;
        NonZeroUsize::new(v)
            .map(|v| Wrap(v))
            .ok_or(RbValueError::new_err("must be non-zero".into()))
    }
}

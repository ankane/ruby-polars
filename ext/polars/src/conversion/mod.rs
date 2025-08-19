pub(crate) mod any_value;
mod categorical;
mod chunked_array;

use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

pub use categorical::RbCategories;
use magnus::{
    IntoValue, Module, RArray, RHash, Ruby, Symbol, TryConvert, Value, class, exception,
    prelude::*, r_hash::ForEach, try_convert::TryConvertOwned, value::Opaque,
};
use polars::chunked_array::object::PolarsObjectSafe;
use polars::chunked_array::ops::{FillNullLimit, FillNullStrategy};
use polars::datatypes::AnyValue;
use polars::frame::row::Row;
use polars::io::avro::AvroCompression;
use polars::io::cloud::CloudOptions;
use polars::prelude::deletion::DeletionFilesList;
use polars::prelude::*;
use polars::series::ops::NullBehavior;
use polars_core::schema::iceberg::IcebergSchema;
use polars_core::utils::arrow::array::Array;
use polars_core::utils::materialize_dyn_int;
use polars_plan::dsl::ScanSources;
use polars_utils::mmap::MemSlice;
use polars_utils::total_ord::{TotalEq, TotalHash};

use crate::file::{RubyScanSourceInput, get_ruby_scan_source_input};
use crate::object::OBJECT_NAME;
use crate::rb_modules::series;
use crate::utils::to_rb_err;
use crate::{RbDataFrame, RbLazyFrame, RbPolarsErr, RbResult, RbSeries, RbTypeError, RbValueError};

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
    Ok(rbdf.ldf.borrow().clone())
}

pub(crate) fn get_series(obj: Value) -> RbResult<Series> {
    let rbs = obj.funcall::<_, _, &RbSeries>("_s", ())?;
    Ok(rbs.series.borrow().clone())
}

pub(crate) fn to_series(s: RbSeries) -> Value {
    let series = series();
    series
        .funcall::<_, _, Value>("_from_rbseries", (s,))
        .unwrap()
}

impl TryConvert for Wrap<PlSmallStr> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Wrap((&*String::try_convert(ob)?).into()))
    }
}

impl TryConvert for Wrap<NullValues> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if let Ok(s) = String::try_convert(ob) {
            Ok(Wrap(NullValues::AllColumnsSingle((&*s).into())))
        } else if let Ok(s) = Vec::<String>::try_convert(ob) {
            Ok(Wrap(NullValues::AllColumns(
                s.into_iter().map(|x| (&*x).into()).collect(),
            )))
        } else if let Ok(s) = Vec::<(String, String)>::try_convert(ob) {
            Ok(Wrap(NullValues::Named(
                s.into_iter()
                    .map(|(a, b)| ((&*a).into(), (&*b).into()))
                    .collect(),
            )))
        } else {
            Err(
                RbPolarsErr::Other("could not extract value from null_values argument".into())
                    .into(),
            )
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
            DataType::Int8 => {
                let class = pl.const_get::<_, Value>("Int8").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Int16 => {
                let class = pl.const_get::<_, Value>("Int16").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Int32 => {
                let class = pl.const_get::<_, Value>("Int32").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Int64 => {
                let class = pl.const_get::<_, Value>("Int64").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Int128 => {
                let class = pl.const_get::<_, Value>("Int128").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::UInt8 => {
                let class = pl.const_get::<_, Value>("UInt8").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::UInt16 => {
                let class = pl.const_get::<_, Value>("UInt16").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::UInt32 => {
                let class = pl.const_get::<_, Value>("UInt32").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::UInt64 => {
                let class = pl.const_get::<_, Value>("UInt64").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Float32 => {
                let class = pl.const_get::<_, Value>("Float32").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Float64 | DataType::Unknown(UnknownKind::Float) => {
                let class = pl.const_get::<_, Value>("Float64").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Decimal(precision, scale) => {
                let class = pl.const_get::<_, Value>("Decimal").unwrap();
                class
                    .funcall::<_, _, Value>("new", (precision, scale))
                    .unwrap()
            }
            DataType::Boolean => {
                let class = pl.const_get::<_, Value>("Boolean").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::String | DataType::Unknown(UnknownKind::Str) => {
                let class = pl.const_get::<_, Value>("String").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Binary => {
                let class = pl.const_get::<_, Value>("Binary").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Array(inner, size) => {
                let class = pl.const_get::<_, Value>("Array").unwrap();
                let inner = Wrap(*inner);
                let args = (inner, size);
                class.funcall::<_, _, Value>("new", args).unwrap()
            }
            DataType::List(inner) => {
                let class = pl.const_get::<_, Value>("List").unwrap();
                let inner = Wrap(*inner);
                class.funcall::<_, _, Value>("new", (inner,)).unwrap()
            }
            DataType::Date => {
                let class = pl.const_get::<_, Value>("Date").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Datetime(tu, tz) => {
                let datetime_class = pl.const_get::<_, Value>("Datetime").unwrap();
                datetime_class
                    .funcall::<_, _, Value>(
                        "new",
                        (tu.to_ascii(), tz.as_deref().map(|x| x.as_str())),
                    )
                    .unwrap()
            }
            DataType::Duration(tu) => {
                let duration_class = pl.const_get::<_, Value>("Duration").unwrap();
                duration_class
                    .funcall::<_, _, Value>("new", (tu.to_ascii(),))
                    .unwrap()
            }
            DataType::Object(_) => {
                let class = pl.const_get::<_, Value>("Object").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Categorical(cats, _) => {
                let categories_class = pl.const_get::<_, Value>("Categories").unwrap();
                let categorical_class = pl.const_get::<_, Value>("Categorical").unwrap();
                let categories: Value = categories_class
                    .funcall("_from_rb_categories", (RbCategories::from(cats.clone()),))
                    .unwrap();
                let kwargs = RHash::new();
                kwargs.aset(Symbol::new("categories"), categories).unwrap();
                categorical_class.funcall("new", (kwargs,)).unwrap()
            }
            DataType::Enum(_, mapping) => {
                let categories = unsafe {
                    StringChunked::from_chunks(
                        PlSmallStr::from_static("category"),
                        vec![mapping.to_arrow(true)],
                    )
                };
                let class = pl.const_get::<_, Value>("Enum").unwrap();
                let series = to_series(categories.into_series().into());
                class.funcall::<_, _, Value>("new", (series,)).unwrap()
            }
            DataType::Time => {
                let class = pl.const_get::<_, Value>("Time").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Struct(fields) => {
                let field_class = pl.const_get::<_, Value>("Field").unwrap();
                let iter = fields.iter().map(|fld| {
                    let name = fld.name().as_str();
                    let dtype = Wrap(fld.dtype().clone());
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
            DataType::Null => {
                let class = pl.const_get::<_, Value>("Null").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::Unknown(UnknownKind::Int(v)) => {
                Wrap(materialize_dyn_int(v).dtype()).into_value()
            }
            DataType::Unknown(_) => {
                let class = pl.const_get::<_, Value>("Unknown").unwrap();
                class.funcall("new", ()).unwrap()
            }
            DataType::BinaryOffset => {
                unimplemented!()
            }
        }
    }
}

enum CategoricalOrdering {
    Lexical,
}

impl IntoValue for Wrap<CategoricalOrdering> {
    fn into_value_with(self, _: &Ruby) -> Value {
        "lexical".into_value()
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
        Ok(Wrap(Field::new((&*name).into(), dtype.0)))
    }
}

impl TryConvert for Wrap<DataType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let dtype = if ob.is_kind_of(class::class()) {
            let name = ob.funcall::<_, _, String>("name", ())?;
            match name.as_str() {
                "Polars::Int8" => DataType::Int8,
                "Polars::Int16" => DataType::Int16,
                "Polars::Int32" => DataType::Int32,
                "Polars::Int64" => DataType::Int64,
                "Polars::UInt8" => DataType::UInt8,
                "Polars::UInt16" => DataType::UInt16,
                "Polars::UInt32" => DataType::UInt32,
                "Polars::UInt64" => DataType::UInt64,
                "Polars::Float32" => DataType::Float32,
                "Polars::Float64" => DataType::Float64,
                "Polars::Boolean" => DataType::Boolean,
                "Polars::String" => DataType::String,
                "Polars::Binary" => DataType::Binary,
                "Polars::Categorical" => DataType::from_categories(Categories::global()),
                "Polars::Enum" => {
                    DataType::from_frozen_categories(FrozenCategories::new([]).unwrap())
                }
                "Polars::Date" => DataType::Date,
                "Polars::Time" => DataType::Time,
                "Polars::Datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
                "Polars::Duration" => DataType::Duration(TimeUnit::Microseconds),
                "Polars::Decimal" => DataType::Decimal(None, None),
                "Polars::List" => DataType::List(Box::new(DataType::Null)),
                "Polars::Array" => DataType::Array(Box::new(DataType::Null), 0),
                "Polars::Struct" => DataType::Struct(vec![]),
                "Polars::Null" => DataType::Null,
                "Polars::Object" => DataType::Object(OBJECT_NAME),
                "Polars::Unknown" => DataType::Unknown(Default::default()),
                dt => {
                    return Err(RbValueError::new_err(format!(
                        "{dt} is not a correct polars DataType.",
                    )));
                }
            }
        } else if String::try_convert(ob).is_err() {
            let cls = ob.class();
            let name = unsafe { cls.name() }.into_owned();
            match name.as_str() {
                "Polars::Int8" => DataType::Int8,
                "Polars::Int16" => DataType::Int16,
                "Polars::Int32" => DataType::Int32,
                "Polars::Int64" => DataType::Int64,
                "Polars::UInt8" => DataType::UInt8,
                "Polars::UInt16" => DataType::UInt16,
                "Polars::UInt32" => DataType::UInt32,
                "Polars::UInt64" => DataType::UInt64,
                "Polars::Float32" => DataType::Float32,
                "Polars::Float64" => DataType::Float64,
                "Polars::Boolean" => DataType::Boolean,
                "Polars::String" => DataType::String,
                "Polars::Binary" => DataType::Binary,
                "Polars::Categorical" => {
                    let categories: Value = ob.funcall("categories", ()).unwrap();
                    let rb_categories: &RbCategories =
                        categories.funcall("_categories", ()).unwrap();
                    DataType::from_categories(rb_categories.categories().clone())
                }
                "Polars::Enum" => {
                    let categories: Value = ob.funcall("categories", ()).unwrap();
                    let s = get_series(categories)?;
                    let ca = s.str().map_err(RbPolarsErr::from)?;
                    let categories = ca.downcast_iter().next().unwrap().clone();
                    assert!(!categories.has_nulls());
                    DataType::from_frozen_categories(
                        FrozenCategories::new(categories.values_iter()).unwrap(),
                    )
                }
                "Polars::Date" => DataType::Date,
                "Polars::Time" => DataType::Time,
                "Polars::Datetime" => {
                    let time_unit: Value = ob.funcall("time_unit", ()).unwrap();
                    let time_unit = Wrap::<TimeUnit>::try_convert(time_unit)?.0;
                    let time_zone: Option<String> = ob.funcall("time_zone", ())?;
                    DataType::Datetime(
                        time_unit,
                        TimeZone::opt_try_new(time_zone.as_deref()).map_err(RbPolarsErr::from)?,
                    )
                }
                "Polars::Duration" => {
                    let time_unit: Value = ob.funcall("time_unit", ()).unwrap();
                    let time_unit = Wrap::<TimeUnit>::try_convert(time_unit)?.0;
                    DataType::Duration(time_unit)
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
                "Polars::Array" => {
                    let inner: Value = ob.funcall("inner", ()).unwrap();
                    let size: Value = ob.funcall("size", ()).unwrap();
                    let inner = Wrap::<DataType>::try_convert(inner)?;
                    let size = usize::try_convert(size)?;
                    DataType::Array(Box::new(inner.0), size)
                }
                "Polars::Struct" => {
                    let arr: RArray = ob.funcall("fields", ())?;
                    let mut fields = Vec::with_capacity(arr.len());
                    for v in arr.into_iter() {
                        fields.push(Wrap::<Field>::try_convert(v)?.0);
                    }
                    DataType::Struct(fields)
                }
                "Polars::Null" => DataType::Null,
                "Object" => DataType::Object(OBJECT_NAME),
                "Polars::Unknown" => DataType::Unknown(Default::default()),
                dt => {
                    return Err(RbTypeError::new_err(format!(
                        "A {dt} object is not a correct polars DataType. \
                        Hint: use the class without instantiating it.",
                    )));
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
                "cat" => DataType::from_categories(Categories::global()),
                "date" => DataType::Date,
                "datetime" => DataType::Datetime(TimeUnit::Microseconds, None),
                "f32" => DataType::Float32,
                "time" => DataType::Time,
                "dur" => DataType::Duration(TimeUnit::Microseconds),
                "f64" => DataType::Float64,
                "obj" => DataType::Object(OBJECT_NAME),
                "list" => DataType::List(Box::new(DataType::Boolean)),
                "null" => DataType::Null,
                "unk" => DataType::Unknown(Default::default()),
                _ => {
                    return Err(RbValueError::new_err(format!(
                        "{ob} is not a supported DataType."
                    )));
                }
            }
        };
        Ok(Wrap(dtype))
    }
}

unsafe impl TryConvertOwned for Wrap<DataType> {}

impl TryConvert for Wrap<StatisticsOptions> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let mut statistics = StatisticsOptions::empty();

        let dict = RHash::try_convert(ob)?;
        dict.foreach(|key: Symbol, val: bool| {
            match key.name()?.as_ref() {
                "min" => statistics.min_value = val,
                "max" => statistics.max_value = val,
                "distinct_count" => statistics.distinct_count = val,
                "null_count" => statistics.null_count = val,
                _ => {
                    return Err(RbTypeError::new_err(format!(
                        "'{key}' is not a valid statistic option",
                    )));
                }
            }
            Ok(ForEach::Continue)
        })?;

        Ok(Wrap(statistics))
    }
}

impl<'s> TryConvert for Wrap<Row<'s>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let mut vals: Vec<Wrap<AnyValue<'s>>> = Vec::new();
        for item in RArray::try_convert(ob)?.into_iter() {
            vals.push(Wrap::<AnyValue<'s>>::try_convert(item)?);
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
            schema.push(Ok(Field::new((&*key).into(), val.0)));
            Ok(ForEach::Continue)
        })?;

        Ok(Wrap(schema.into_iter().collect::<RbResult<Schema>>()?))
    }
}

impl TryConvert for Wrap<ArrowSchema> {
    fn try_convert(_ob: Value) -> RbResult<Self> {
        todo!();
    }
}

impl TryConvert for Wrap<ScanSources> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let list = RArray::try_convert(ob)?;

        if list.is_empty() {
            return Ok(Wrap(ScanSources::default()));
        }

        enum MutableSources {
            Paths(Vec<PlPath>),
            Files(Vec<File>),
            Buffers(Vec<MemSlice>),
        }

        let num_items = list.len();
        let mut iter = list
            .into_iter()
            .map(|val| get_ruby_scan_source_input(val, false));

        let Some(first) = iter.next() else {
            return Ok(Wrap(ScanSources::default()));
        };

        let mut sources = match first? {
            RubyScanSourceInput::Path(path) => {
                let mut sources = Vec::with_capacity(num_items);
                sources.push(path);
                MutableSources::Paths(sources)
            }
            RubyScanSourceInput::File(file) => {
                let mut sources = Vec::with_capacity(num_items);
                sources.push(file);
                MutableSources::Files(sources)
            }
            RubyScanSourceInput::Buffer(buffer) => {
                let mut sources = Vec::with_capacity(num_items);
                sources.push(buffer);
                MutableSources::Buffers(sources)
            }
        };

        for source in iter {
            match (&mut sources, source?) {
                (MutableSources::Paths(v), RubyScanSourceInput::Path(p)) => v.push(p),
                (MutableSources::Files(v), RubyScanSourceInput::File(f)) => v.push(f),
                (MutableSources::Buffers(v), RubyScanSourceInput::Buffer(f)) => v.push(f),
                _ => {
                    return Err(RbTypeError::new_err(
                        "Cannot combine in-memory bytes, paths and files for scan sources"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(Wrap(match sources {
            MutableSources::Paths(i) => ScanSources::Paths(i.into()),
            MutableSources::Files(i) => ScanSources::Files(i.into()),
            MutableSources::Buffers(i) => ScanSources::Buffers(i.into()),
        }))
    }
}

#[derive(Clone)]
pub struct ObjectValue {
    pub inner: Opaque<Value>,
}

impl Debug for ObjectValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectValue")
            .field("inner", &self.to_value())
            .finish()
    }
}

impl Hash for ObjectValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let h = self
            .to_value()
            .funcall::<_, _, isize>("hash", ())
            .expect("should be hashable");
        state.write_isize(h)
    }
}

impl Eq for ObjectValue {}

impl PartialEq for ObjectValue {
    fn eq(&self, other: &Self) -> bool {
        self.to_value().eql(other.to_value()).unwrap_or(false)
    }
}

impl TotalEq for ObjectValue {
    fn tot_eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl TotalHash for ObjectValue {
    fn tot_hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.hash(state);
    }
}

impl Display for ObjectValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_value())
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

impl ObjectValue {
    pub fn to_value(&self) -> Value {
        self.clone().into_value()
    }
}

impl IntoValue for ObjectValue {
    fn into_value_with(self, ruby: &Ruby) -> Value {
        ruby.get_inner(self.inner)
    }
}

impl Default for ObjectValue {
    fn default() -> Self {
        ObjectValue {
            inner: Ruby::get().unwrap().qnil().as_value().into(),
        }
    }
}

impl TryConvert for Wrap<AsofStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "backward" => AsofStrategy::Backward,
            "forward" => AsofStrategy::Forward,
            "nearest" => AsofStrategy::Nearest,
            v => {
                return Err(RbValueError::new_err(format!(
                    "asof `strategy` must be one of {{'backward', 'forward', 'nearest'}}, got {v}",
                )));
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
                )));
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
                    "compression must be one of {{'uncompressed', 'snappy', 'deflate'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<CategoricalOrdering> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "lexical" => CategoricalOrdering::Lexical,
            "physical" => {
                polars_warn!(
                    Deprecation,
                    "physical ordering is deprecated, will use lexical ordering instead"
                );
                CategoricalOrdering::Lexical
            }
            v => {
                return Err(RbValueError::new_err(format!(
                    "ordering must be one of {{'physical', 'lexical'}}, got {v}"
                )));
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
                )));
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
                    "closed must be one of {{'left', 'right', 'both', 'none'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<RoundMode> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "half_to_even" => RoundMode::HalfToEven,
            "half_away_from_zero" => RoundMode::HalfAwayFromZero,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`mode` must be one of {{'half_to_even', 'half_away_from_zero'}}, got {v}",
                )));
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
                    "encoding must be one of {{'utf8', 'utf8-lossy'}}, got {v}"
                )));
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
                    "compression must be one of {{'uncompressed', 'lz4', 'zstd'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<JoinType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "full" => JoinType::Full,
            "semi" => JoinType::Semi,
            "anti" => JoinType::Anti,
            "cross" => JoinType::Cross,
            v => {
                return Err(RbValueError::new_err(format!(
                    "how must be one of {{'inner', 'left', 'full', 'semi', 'anti', 'cross'}}, got {v}"
                )));
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
                )));
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
                    "n_field_strategy must be one of {{'first_non_null', 'max_width'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<NonExistent> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "null" => NonExistent::Null,
            "raise" => NonExistent::Raise,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`non_existent` must be one of {{'null', 'raise'}}, got {v}",
                )));
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
                    "null behavior must be one of {{'drop', 'ignore'}}, got {v}"
                )));
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
                    "null strategy must be one of {{'ignore', 'propagate'}}, got {v}"
                )));
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
                    "parallel must be one of {{'auto', 'columns', 'row_groups', 'none'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<QuantileMethod> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "lower" => QuantileMethod::Lower,
            "higher" => QuantileMethod::Higher,
            "nearest" => QuantileMethod::Nearest,
            "linear" => QuantileMethod::Linear,
            "midpoint" => QuantileMethod::Midpoint,
            v => {
                return Err(RbValueError::new_err(format!(
                    "interpolation must be one of {{'lower', 'higher', 'nearest', 'linear', 'midpoint'}}, got {v}"
                )));
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
                    "method must be one of {{'min', 'max', 'average', 'dense', 'ordinal', 'random'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<Roll> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "raise" => Roll::Raise,
            "forward" => Roll::Forward,
            "backward" => Roll::Backward,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`roll` must be one of {{'raise', 'forward', 'backward'}}, got {v}",
                )));
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
                    "time unit must be one of {{'ns', 'us', 'ms'}}, got {v}"
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

unsafe impl TryConvertOwned for Wrap<TimeUnit> {}

impl TryConvert for Wrap<UniqueKeepStrategy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "first" => UniqueKeepStrategy::First,
            "last" => UniqueKeepStrategy::Last,
            v => {
                return Err(RbValueError::new_err(format!(
                    "keep must be one of {{'first', 'last'}}, got {v}"
                )));
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
                    "compression must be one of {{'lz4', 'zstd'}}, got {v}"
                )));
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
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<ClosedInterval> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "both" => ClosedInterval::Both,
            "left" => ClosedInterval::Left,
            "right" => ClosedInterval::Right,
            "none" => ClosedInterval::None,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`closed` must be one of {{'both', 'left', 'right', 'none'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<WindowMapping> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "group_to_rows" => WindowMapping::GroupsToRows,
            "join" => WindowMapping::Join,
            "explode" => WindowMapping::Explode,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`mapping_strategy` must be one of {{'group_to_rows', 'join', 'explode'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<JoinValidation> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "1:1" => JoinValidation::OneToOne,
            "1:m" => JoinValidation::OneToMany,
            "m:m" => JoinValidation::ManyToMany,
            "m:1" => JoinValidation::ManyToOne,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`validate` must be one of {{'m:m', 'm:1', '1:m', '1:1'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<MaintainOrderJoin> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "none" => MaintainOrderJoin::None,
            "left" => MaintainOrderJoin::Left,
            "right" => MaintainOrderJoin::Right,
            "left_right" => MaintainOrderJoin::LeftRight,
            "right_left" => MaintainOrderJoin::RightLeft,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`maintain_order` must be one of {{'none', 'left', 'right', 'left_right', 'right_left'}}, got {v}",
                )));
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
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

pub(crate) fn parse_cloud_options(uri: &str, kv: Vec<(String, String)>) -> RbResult<CloudOptions> {
    let out = CloudOptions::from_untyped_config(uri, kv).map_err(RbPolarsErr::from)?;
    Ok(out)
}

impl TryConvert for Wrap<SetOperation> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "union" => SetOperation::Union,
            "difference" => SetOperation::Difference,
            "intersection" => SetOperation::Intersection,
            "symmetric_difference" => SetOperation::SymmetricDifference,
            v => {
                return Err(RbValueError::new_err(format!(
                    "set operation must be one of {{'union', 'difference', 'intersection', 'symmetric_difference'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<CastColumnsPolicy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if ob.is_nil() {
            let out = Wrap(CastColumnsPolicy::ERROR_ON_MISMATCH);
            return Ok(out);
        }
        todo!();
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
            return Err(magnus::Error::new(
                exception::runtime_error(),
                format!(
                    "strategy must be one of {{'forward', 'backward', 'min', 'max', 'mean', 'zero', 'one'}}, got {e}",
                ),
            ));
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
                        .map_err(|e| RbValueError::new_err(format!("{e:?}")))
                })
                .transpose()?,
        ),
        "lzo" => ParquetCompression::Lzo,
        "brotli" => ParquetCompression::Brotli(
            compression_level
                .map(|lvl| {
                    BrotliLevel::try_new(lvl as u32)
                        .map_err(|e| RbValueError::new_err(format!("{e:?}")))
                })
                .transpose()?,
        ),
        "lz4" => ParquetCompression::Lz4Raw,
        "zstd" => ParquetCompression::Zstd(
            compression_level
                .map(|lvl| {
                    ZstdLevel::try_new(lvl).map_err(|e| RbValueError::new_err(format!("{e:?}")))
                })
                .transpose()?,
        ),
        e => {
            return Err(RbValueError::new_err(format!(
                "compression must be one of {{'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'zstd'}}, got {e}"
            )));
        }
    };
    Ok(parsed)
}

impl TryConvert for Wrap<NonZeroUsize> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let v = usize::try_convert(ob)?;
        NonZeroUsize::new(v)
            .map(Wrap)
            .ok_or(RbValueError::new_err("must be non-zero"))
    }
}

pub(crate) fn strings_to_pl_smallstr<I, S>(container: I) -> Vec<PlSmallStr>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    container
        .into_iter()
        .map(|s| PlSmallStr::from_str(s.as_ref()))
        .collect()
}

#[derive(Debug, Copy, Clone)]
pub struct RbCompatLevel(pub CompatLevel);

impl TryConvert for RbCompatLevel {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(RbCompatLevel(if let Ok(level) = u16::try_convert(ob) {
            if let Ok(compat_level) = CompatLevel::with_level(level) {
                compat_level
            } else {
                return Err(RbValueError::new_err("invalid compat level".to_string()));
            }
        } else if let Ok(future) = bool::try_convert(ob) {
            if future {
                CompatLevel::newest()
            } else {
                CompatLevel::oldest()
            }
        } else {
            return Err(RbTypeError::new_err(
                "'compat_level' argument accepts int or bool".to_string(),
            ));
        }))
    }
}

impl TryConvert for Wrap<UnicodeForm> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "NFC" => UnicodeForm::NFC,
            "NFKC" => UnicodeForm::NFKC,
            "NFD" => UnicodeForm::NFD,
            "NFKD" => UnicodeForm::NFKD,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`form` must be one of {{'NFC', 'NFKC', 'NFD', 'NFKD'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<Option<TimeZone>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let tz = Option::<Wrap<PlSmallStr>>::try_convert(ob)?;

        let tz = tz.map(|x| x.0);

        Ok(Wrap(TimeZone::opt_try_new(tz).map_err(RbPolarsErr::from)?))
    }
}

unsafe impl TryConvertOwned for Wrap<Option<TimeZone>> {}

impl TryConvert for Wrap<ExtraColumnsPolicy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "ignore" => ExtraColumnsPolicy::Ignore,
            "raise" => ExtraColumnsPolicy::Raise,
            v => {
                return Err(RbValueError::new_err(format!(
                    "extra column/field parameter must be one of {{'ignore', 'raise'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<MissingColumnsPolicy> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "insert" => MissingColumnsPolicy::Insert,
            "raise" => MissingColumnsPolicy::Raise,
            v => {
                return Err(RbValueError::new_err(format!(
                    "missing column/field parameter must be one of {{'insert', 'raise'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<ColumnMapping> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let (column_mapping_type, ob) = <(String, Value)>::try_convert(ob)?;

        Ok(Wrap(match column_mapping_type.as_str() {
            "iceberg-column-mapping" => {
                let arrow_schema = Wrap::<ArrowSchema>::try_convert(ob)?;
                ColumnMapping::Iceberg(Arc::new(
                    IcebergSchema::from_arrow_schema(&arrow_schema.0).map_err(to_rb_err)?,
                ))
            }

            v => {
                return Err(RbValueError::new_err(format!(
                    "unknown column mapping type: {v}"
                )));
            }
        }))
    }
}

impl TryConvert for Wrap<DeletionFilesList> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let (deletion_file_type, ob) = <(String, Value)>::try_convert(ob)?;

        Ok(Wrap(match deletion_file_type.as_str() {
            "iceberg-position-delete" => {
                let dict = RHash::try_convert(ob)?;

                let mut out = PlIndexMap::new();

                dict.foreach(|k: usize, v: RArray| {
                    let files = v
                        .into_iter()
                        .map(|x| {
                            let x = String::try_convert(x)?;
                            Ok(x)
                        })
                        .collect::<RbResult<Arc<[String]>>>()?;

                    if !files.is_empty() {
                        out.insert(k, files);
                    }

                    Ok(ForEach::Continue)
                })?;

                DeletionFilesList::IcebergPositionDelete(Arc::new(out))
            }

            v => {
                return Err(RbValueError::new_err(format!(
                    "unknown deletion file type: {v}"
                )));
            }
        }))
    }
}

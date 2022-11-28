use magnus::{class, RArray, Symbol, TryConvert, Value, QNIL};
use polars::chunked_array::object::PolarsObjectSafe;
use polars::chunked_array::ops::{FillNullLimit, FillNullStrategy};
use polars::datatypes::AnyValue;
use polars::frame::DataFrame;
use polars::prelude::*;
use polars::series::ops::NullBehavior;

use crate::{RbDataFrame, RbPolarsErr, RbResult, RbSeries, RbValueError};

pub struct Wrap<T>(pub T);

impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}

pub fn get_rbseq(obj: Value) -> RbResult<(RArray, usize)> {
    let seq: RArray = obj.try_convert()?;
    let len = seq.len();
    Ok((seq, len))
}

pub fn get_df(obj: Value) -> RbResult<DataFrame> {
    let rbdf = obj.funcall::<_, _, &RbDataFrame>("_df", ())?;
    Ok(rbdf.df.borrow().clone())
}

pub fn get_series(obj: Value) -> RbResult<Series> {
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

impl From<Wrap<AnyValue<'_>>> for Value {
    fn from(w: Wrap<AnyValue<'_>>) -> Self {
        match w.0 {
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
            AnyValue::Date(v) => class::time()
                .funcall::<_, _, Value>("at", (v * 86400,))
                .unwrap()
                .funcall::<_, _, Value>("utc", ())
                .unwrap()
                .funcall::<_, _, Value>("to_date", ())
                .unwrap(),
            _ => todo!(),
        }
    }
}

impl From<Wrap<DataType>> for Value {
    fn from(w: Wrap<DataType>) -> Self {
        Symbol::from(w.0.to_string()).into()
    }
}

impl TryConvert for Wrap<DataType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let dtype = match ob.try_convert::<String>()?.as_str() {
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
        };
        Ok(Wrap(dtype))
    }
}

impl<'s> TryConvert for Wrap<AnyValue<'s>> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        // TODO improve
        if let Ok(v) = ob.try_convert::<i64>() {
            Ok(AnyValue::Int64(v).into())
        } else if let Ok(v) = ob.try_convert::<f64>() {
            Ok(AnyValue::Float64(v).into())
        } else {
            Err(RbPolarsErr::other(format!(
                "object type not supported {:?}",
                ob
            )))
        }
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
            return Err(magnus::Error::runtime_error(format!(
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

pub struct ObjectValue {
    pub inner: Value,
}

impl From<&dyn PolarsObjectSafe> for &ObjectValue {
    fn from(val: &dyn PolarsObjectSafe) -> Self {
        unsafe { &*(val as *const dyn PolarsObjectSafe as *const ObjectValue) }
    }
}

impl ObjectValue {
    pub fn to_object(&self) -> Value {
        self.inner
    }
}

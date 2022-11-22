use magnus::{Value, QNIL};
use polars::chunked_array::ops::{FillNullLimit, FillNullStrategy};
use polars::datatypes::AnyValue;
use polars::frame::DataFrame;
use polars::prelude::*;

use crate::{RbDataFrame, RbResult, RbValueError};

pub fn wrap(val: AnyValue) -> Value {
    match val {
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
        _ => todo!(),
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

pub fn wrap_join_type(ob: &str) -> RbResult<JoinType> {
    let parsed = match ob {
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
    Ok(parsed)
}

pub fn get_df(obj: Value) -> RbResult<DataFrame> {
    let rbdf = obj.funcall::<_, _, &RbDataFrame>("_df", ())?;
    Ok(rbdf.df.borrow().clone())
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

pub fn wrap_quantile_interpol_options(ob: &str) -> RbResult<QuantileInterpolOptions> {
    let parsed = match ob {
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
    Ok(parsed)
}

use arrow::array::Array;
use polars::prelude::*;

use crate::error::RbPolarsErr;
use crate::{RbErr, RbResult, RbSeries};

impl RbSeries {
    pub fn scatter(&self, idx: &RbSeries, values: &RbSeries) -> RbResult<()> {
        let mut s = self.series.borrow_mut();
        match scatter(s.clone(), &idx.series.borrow(), &values.series.borrow()) {
            Ok(out) => {
                *s = out;
                Ok(())
            }
            Err(e) => Err(RbErr::from(RbPolarsErr::from(e))),
        }
    }
}

fn scatter(mut s: Series, idx: &Series, values: &Series) -> PolarsResult<Series> {
    let logical_dtype = s.dtype().clone();
    let idx = idx.cast(&IDX_DTYPE)?;
    let idx = idx.rechunk();
    let idx = idx.idx().unwrap();
    let idx = idx.downcast_iter().next().unwrap();

    if idx.null_count() > 0 {
        return Err(PolarsError::ComputeError(
            "index values should not be null".into(),
        ));
    }

    let idx = idx.values().as_slice();

    let values = values.to_physical_repr().cast(&s.dtype().to_physical())?;

    // do not shadow, otherwise s is not dropped immediately
    // and we want to have mutable access
    s = s.to_physical_repr().into_owned();
    let mutable_s = s._get_inner_mut();

    let s = match logical_dtype.to_physical() {
        DataType::Int8 => {
            let ca: &mut ChunkedArray<Int8Type> = mutable_s.as_mut();
            let values = values.i8()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::Int16 => {
            let ca: &mut ChunkedArray<Int16Type> = mutable_s.as_mut();
            let values = values.i16()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::Int32 => {
            let ca: &mut ChunkedArray<Int32Type> = mutable_s.as_mut();
            let values = values.i32()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::Int64 => {
            let ca: &mut ChunkedArray<Int64Type> = mutable_s.as_mut();
            let values = values.i64()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::UInt8 => {
            let ca: &mut ChunkedArray<UInt8Type> = mutable_s.as_mut();
            let values = values.u8()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::UInt16 => {
            let ca: &mut ChunkedArray<UInt16Type> = mutable_s.as_mut();
            let values = values.u16()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::UInt32 => {
            let ca: &mut ChunkedArray<UInt32Type> = mutable_s.as_mut();
            let values = values.u32()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::UInt64 => {
            let ca: &mut ChunkedArray<UInt64Type> = mutable_s.as_mut();
            let values = values.u64()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::Float32 => {
            let ca: &mut ChunkedArray<Float32Type> = mutable_s.as_mut();
            let values = values.f32()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::Float64 => {
            let ca: &mut ChunkedArray<Float64Type> = mutable_s.as_mut();
            let values = values.f64()?;
            std::mem::take(ca).scatter(idx, values)
        }
        DataType::Boolean => {
            let ca = s.bool()?;
            let values = values.bool()?;
            ca.scatter(idx, values)
        }
        DataType::String => {
            let ca = s.str()?;
            let values = values.str()?;
            ca.scatter(idx, values)
        }
        _ => panic!("not yet implemented for dtype: {}", logical_dtype),
    };

    s.and_then(|s| s.cast(&logical_dtype))
}

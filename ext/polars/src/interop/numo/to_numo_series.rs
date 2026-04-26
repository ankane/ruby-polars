use magnus::{Ruby, Value};
use num_traits::{Float, NumCast};
use polars_core::prelude::*;

use crate::RbResult;
use crate::error::RbPolarsErr;
use crate::raise_err;
use crate::ruby::numo::{Element, RbArray1};
use crate::series::RbSeries;

impl RbSeries {
    /// Convert this Series to a Numo array.
    pub fn to_numo(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        series_to_numo(rb, &self_.series.read())
    }
}

/// Convert a Series to a Numo array.
fn series_to_numo(rb: &Ruby, s: &Series) -> RbResult<Value> {
    series_to_numo_with_copy(rb, s)
}

/// Convert a Series to a Numo array, copying data in the process.
fn series_to_numo_with_copy(rb: &Ruby, s: &Series) -> RbResult<Value> {
    use DataType::*;
    match s.dtype() {
        Int8 => numeric_series_to_numo::<Int8Type, f32>(rb, s),
        Int16 => numeric_series_to_numo::<Int16Type, f32>(rb, s),
        Int32 => numeric_series_to_numo::<Int32Type, f64>(rb, s),
        Int64 => numeric_series_to_numo::<Int64Type, f64>(rb, s),
        UInt8 => numeric_series_to_numo::<UInt8Type, f32>(rb, s),
        UInt16 => numeric_series_to_numo::<UInt16Type, f32>(rb, s),
        UInt32 => numeric_series_to_numo::<UInt32Type, f64>(rb, s),
        UInt64 => numeric_series_to_numo::<UInt64Type, f64>(rb, s),
        Float32 => numeric_series_to_numo::<Float32Type, f32>(rb, s),
        Float64 => numeric_series_to_numo::<Float64Type, f64>(rb, s),
        Boolean => boolean_series_to_numo(rb, s),
        String => {
            let ca = s.str().unwrap();
            RbArray1::from_iter(rb, ca)
        }
        dt => {
            raise_err!(
                format!("'to_numo' not supported for dtype: {dt:?}"),
                ComputeError
            );
        }
    }
}

/// Convert numeric types to f32 or f64 with NaN representing a null value.
fn numeric_series_to_numo<T, U>(rb: &Ruby, s: &Series) -> RbResult<Value>
where
    T: PolarsNumericType,
    T::Native: Element,
    U: Float + Element,
{
    let ca: &ChunkedArray<T> = s.as_ref().as_ref();
    if s.null_count() == 0 {
        let values = ca.into_no_null_iter();
        RbArray1::<T::Native>::from_iter(rb, values)
    } else {
        let mapper = |opt_v: Option<T::Native>| match opt_v {
            Some(v) => NumCast::from(v).unwrap(),
            None => U::nan(),
        };
        let values = ca.iter().map(mapper);
        RbArray1::from_iter(rb, values)
    }
}

/// Convert booleans to bit if no nulls are present, otherwise convert to objects.
fn boolean_series_to_numo(rb: &Ruby, s: &Series) -> RbResult<Value> {
    let ca = s.bool().unwrap();
    if s.null_count() == 0 {
        let values = ca.into_no_null_iter();
        RbArray1::<bool>::from_iter(rb, values)
    } else {
        let values = ca.iter();
        RbArray1::from_iter(rb, values)
    }
}

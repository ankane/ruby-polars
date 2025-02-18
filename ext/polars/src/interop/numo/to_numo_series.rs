use magnus::{class, prelude::*, IntoValue, Module, RArray, RClass, RModule, Value};
use num_traits::{Float, NumCast};
use polars_core::prelude::*;

use crate::error::RbPolarsErr;
use crate::raise_err;
use crate::series::RbSeries;
use crate::RbResult;

impl RbSeries {
    /// Convert this Series to a Numo array.
    pub fn to_numo(&self) -> RbResult<Value> {
        series_to_numo(&self.series.borrow())
    }
}

struct RbArray {}

impl RbArray {
    fn from_iter<I>(cls: &str, values: I) -> RbResult<Value>
    where
        I: IntoIterator<Item: IntoValue>,
    {
        class::object()
            .const_get::<_, RModule>("Numo")?
            .const_get::<_, RClass>(cls)?
            .funcall("cast", (RArray::from_iter(values),))
    }
}

/// Convert a Series to a Numo array.
fn series_to_numo(s: &Series) -> RbResult<Value> {
    series_to_numo_with_copy(s)
}

/// Convert a Series to a Numo array, copying data in the process.
fn series_to_numo_with_copy(s: &Series) -> RbResult<Value> {
    use DataType::*;
    match s.dtype() {
        Int8 => numeric_series_to_numpy::<Int8Type, f32>(s, "Int8", "SFloat"),
        Int16 => numeric_series_to_numpy::<Int16Type, f32>(s, "Int16", "SFloat"),
        Int32 => numeric_series_to_numpy::<Int32Type, f64>(s, "Int32", "DFloat"),
        Int64 => numeric_series_to_numpy::<Int64Type, f64>(s, "Int64", "DFloat"),
        UInt8 => numeric_series_to_numpy::<UInt8Type, f32>(s, "UInt8", "SFloat"),
        UInt16 => numeric_series_to_numpy::<UInt16Type, f32>(s, "UInt16", "SFloat"),
        UInt32 => numeric_series_to_numpy::<UInt32Type, f64>(s, "UInt32", "DFloat"),
        UInt64 => numeric_series_to_numpy::<UInt64Type, f64>(s, "UInt64", "DFloat"),
        Float32 => numeric_series_to_numpy::<Float32Type, f32>(s, "SFloat", "SFloat"),
        Float64 => numeric_series_to_numpy::<Float64Type, f64>(s, "DFloat", "DFloat"),
        Boolean => boolean_series_to_numo(s),
        String => {
            let ca = s.str().unwrap();
            RbArray::from_iter("RObject", ca)
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
fn numeric_series_to_numpy<T, U>(s: &Series, c: &str, c2: &str) -> RbResult<Value>
where
    T: PolarsNumericType,
    T::Native: magnus::IntoValue,
    U: Float + magnus::IntoValue,
{
    let ca: &ChunkedArray<T> = s.as_ref().as_ref();
    if s.null_count() == 0 {
        let values = ca.into_no_null_iter();
        RbArray::from_iter(c, values)
    } else {
        let mapper = |opt_v: Option<T::Native>| match opt_v {
            Some(v) => NumCast::from(v).unwrap(),
            None => U::nan(),
        };
        let values = ca.iter().map(mapper);
        RbArray::from_iter(c2, values)
    }
}

/// Convert booleans to bit if no nulls are present, otherwise convert to objects.
fn boolean_series_to_numo(s: &Series) -> RbResult<Value> {
    let ca = s.bool().unwrap();
    if s.null_count() == 0 {
        let values = ca.into_no_null_iter();
        RbArray::from_iter("Bit", values)
    } else {
        let values = ca.iter();
        RbArray::from_iter("RObject", values)
    }
}

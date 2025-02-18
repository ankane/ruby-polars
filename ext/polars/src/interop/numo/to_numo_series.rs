use magnus::{class, prelude::*, Module, RArray, RClass, RModule, Value};
use polars::series::BitRepr;
use polars_core::prelude::*;

use crate::error::RbPolarsErr;
use crate::raise_err;
use crate::series::RbSeries;
use crate::RbResult;

impl RbSeries {
    /// For numeric types, this should only be called for Series with null types.
    /// This will cast to floats so that `nil = NAN`
    pub fn to_numo(&self) -> RbResult<Value> {
        let s = &self.series.borrow();
        match s.dtype() {
            DataType::String => {
                let ca = s.str().unwrap();

                // TODO make more efficient
                let np_arr = RArray::from_iter(ca);
                class::object()
                    .const_get::<_, RModule>("Numo")?
                    .const_get::<_, RClass>("RObject")?
                    .funcall("cast", (np_arr,))
            }
            dt if dt.is_primitive_numeric() => {
                if let Some(BitRepr::Large(_)) = s.bit_repr() {
                    let s = s.cast(&DataType::Float64).unwrap();
                    let ca = s.f64().unwrap();
                    // TODO make more efficient
                    let np_arr = RArray::from_iter(ca.into_iter().map(|opt_v| match opt_v {
                        Some(v) => v,
                        None => f64::NAN,
                    }));
                    class::object()
                        .const_get::<_, RModule>("Numo")?
                        .const_get::<_, RClass>("DFloat")?
                        .funcall("cast", (np_arr,))
                } else {
                    let s = s.cast(&DataType::Float32).unwrap();
                    let ca = s.f32().unwrap();
                    // TODO make more efficient
                    let np_arr = RArray::from_iter(ca.into_iter().map(|opt_v| match opt_v {
                        Some(v) => v,
                        None => f32::NAN,
                    }));
                    class::object()
                        .const_get::<_, RModule>("Numo")?
                        .const_get::<_, RClass>("SFloat")?
                        .funcall("cast", (np_arr,))
                }
            }
            DataType::Boolean => boolean_series_to_numo(s),
            dt => {
                raise_err!(
                    format!("'to_numo' not supported for dtype: {dt:?}"),
                    ComputeError
                );
            }
        }
    }
}

fn boolean_series_to_numo(s: &Series) -> RbResult<Value> {
    let ca = s.bool().unwrap();
    if s.null_count() == 0 {
        let values = ca.into_no_null_iter();
        class::object()
            .const_get::<_, RModule>("Numo")?
            .const_get::<_, RClass>("Bit")?
            .funcall("cast", (RArray::from_iter(values),))
    } else {
        let values = ca.iter();
        class::object()
            .const_get::<_, RModule>("Numo")?
            .const_get::<_, RClass>("RObject")?
            .funcall("cast", (RArray::from_iter(values),))
    }
}

use magnus::{class, Module, RArray, RClass, RModule, Value};
use polars_core::prelude::*;

use crate::{raise_err, RbPolarsErr, RbResult, RbSeries};

impl RbSeries {
    /// For numeric types, this should only be called for Series with null types.
    /// This will cast to floats so that `nil = NAN`
    pub fn to_numo(&self) -> RbResult<Value> {
        let s = &self.series.borrow();
        match s.dtype() {
            DataType::Utf8 => {
                todo!()
            }
            dt if dt.is_numeric() => {
                if s.bit_repr_is_large() {
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
            dt => {
                raise_err!(
                    format!("'to_numo' not supported for dtype: {dt:?}"),
                    ComputeError
                );
            }
        }
    }
}

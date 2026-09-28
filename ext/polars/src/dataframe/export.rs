use magnus::{RArray, Ruby, Value};

use super::*;
use crate::RbResult;
use crate::conversion::{ObjectValue, Wrap};
use crate::error::RbPolarsErr;
use crate::interop::arrow::to_rb::dataframe_to_stream;
use crate::ruby::utils::TryIntoValue;
use crate::utils::EnterPolarsExt;

impl RbDataFrame {
    pub fn row_tuple(ruby: &Ruby, self_: &Self, idx: i64) -> RbResult<RArray> {
        let df = self_.df.read();
        let idx = if idx < 0 {
            (df.height() as i64 + idx) as usize
        } else {
            idx as usize
        };
        if idx >= df.height() {
            return Err(RbPolarsErr::from(polars_err!(oob = idx, df.height())).into());
        }
        ruby.ary_try_from_iter(df.columns().iter().map(|s| match s.dtype() {
            DataType::Object(_) => {
                let obj: Option<&ObjectValue> = s.get_object(idx).map(|any| any.into());
                // TODO remove unwrap and clone
                obj.unwrap().clone().try_into_value_with(ruby)
            }
            _ => Wrap(s.get(idx).unwrap()).try_into_value_with(ruby),
        }))
    }

    pub fn row_tuples(ruby: &Ruby, self_: &Self) -> RbResult<RArray> {
        let df = self_.df.read();
        let mut rechunked;
        let df = if df.max_n_chunks() > 16 {
            rechunked = df.clone();
            ruby.enter_polars_ok(|| rechunked.rechunk_mut_par())?;
            &rechunked
        } else {
            &df
        };
        ruby.ary_try_from_iter((0..df.height()).map(|idx| {
            ruby.ary_try_from_iter(df.columns().iter().map(|s| match s.dtype() {
                DataType::Object(_) => {
                    let obj: Option<&ObjectValue> = s.get_object(idx).map(|any| any.into());
                    // TODO remove unwrap and clone
                    obj.unwrap().clone().try_into_value_with(ruby)
                }
                _ => Wrap(s.get(idx).unwrap()).try_into_value_with(ruby),
            }))
        }))
    }

    pub fn __arrow_c_stream__(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        ruby.enter_polars_ok(|| {
            self_.df.write().rechunk_mut_par();
        })?;
        dataframe_to_stream(&self_.df.read(), ruby)
    }
}

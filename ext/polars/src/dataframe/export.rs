use magnus::{IntoValue, Ruby, Value, prelude::*};

use super::*;
use crate::RbResult;
use crate::conversion::{ObjectValue, Wrap};
use crate::interop::arrow::to_ruby::dataframe_to_stream;

impl RbDataFrame {
    pub fn row_tuple(ruby: &Ruby, self_: &Self, idx: i64) -> Value {
        let idx = if idx < 0 {
            (self_.df.borrow().height() as i64 + idx) as usize
        } else {
            idx as usize
        };
        ruby.ary_from_iter(
            self_
                .df
                .borrow()
                .get_columns()
                .iter()
                .map(|s| match s.dtype() {
                    DataType::Object(_) => {
                        let obj: Option<&ObjectValue> = s.get_object(idx).map(|any| any.into());
                        obj.unwrap().to_value()
                    }
                    _ => Wrap(s.get(idx).unwrap()).into_value_with(ruby),
                }),
        )
        .as_value()
    }

    pub fn row_tuples(ruby: &Ruby, self_: &Self) -> Value {
        let df = &self_.df;
        ruby.ary_from_iter((0..df.borrow().height()).map(|idx| {
            ruby.ary_from_iter(
                self_
                    .df
                    .borrow()
                    .get_columns()
                    .iter()
                    .map(|s| match s.dtype() {
                        DataType::Object(_) => {
                            let obj: Option<&ObjectValue> = s.get_object(idx).map(|any| any.into());
                            obj.unwrap().to_value()
                        }
                        _ => Wrap(s.get(idx).unwrap()).into_value_with(ruby),
                    }),
            )
        }))
        .as_value()
    }

    pub fn __arrow_c_stream__(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        self_.df.borrow_mut().align_chunks();
        dataframe_to_stream(&self_.df.borrow(), ruby)
    }
}

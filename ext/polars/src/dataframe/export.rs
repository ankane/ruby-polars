use magnus::{prelude::*, IntoValue, RArray, Value};

use super::*;
use crate::conversion::{ObjectValue, Wrap};
use crate::interop::arrow::to_ruby::dataframe_to_stream;
use crate::RbResult;

impl RbDataFrame {
    pub fn row_tuple(&self, idx: i64) -> Value {
        let idx = if idx < 0 {
            (self.df.borrow().height() as i64 + idx) as usize
        } else {
            idx as usize
        };
        RArray::from_iter(
            self.df
                .borrow()
                .get_columns()
                .iter()
                .map(|s| match s.dtype() {
                    DataType::Object(_) => {
                        let obj: Option<&ObjectValue> = s.get_object(idx).map(|any| any.into());
                        obj.unwrap().to_value()
                    }
                    _ => Wrap(s.get(idx).unwrap()).into_value(),
                }),
        )
        .as_value()
    }

    pub fn row_tuples(&self) -> Value {
        let df = &self.df;
        RArray::from_iter((0..df.borrow().height()).map(|idx| {
            RArray::from_iter(
                self.df
                    .borrow()
                    .get_columns()
                    .iter()
                    .map(|s| match s.dtype() {
                        DataType::Object(_) => {
                            let obj: Option<&ObjectValue> = s.get_object(idx).map(|any| any.into());
                            obj.unwrap().to_value()
                        }
                        _ => Wrap(s.get(idx).unwrap()).into_value(),
                    }),
            )
        }))
        .as_value()
    }

    pub fn __arrow_c_stream__(&self) -> RbResult<Value> {
        self.df.borrow_mut().align_chunks();
        dataframe_to_stream(&self.df.borrow())
    }
}

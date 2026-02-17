use std::sync::Arc;

use magnus::{Error, Value, gc, value::Opaque};
use polars::error::PolarsError;

pub(crate) fn to_pl_err(e: Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

#[derive(Clone)]
pub struct RubyUdfValue(pub Arc<Opaque<Value>>);

impl RubyUdfValue {
    pub fn new(value: Value) -> Self {
        let ob = Arc::new(Opaque::from(value));
        gc::register_address(&*ob);
        Self(ob)
    }
}

impl Drop for RubyUdfValue {
    fn drop(&mut self) {
        gc::unregister_address(&*self.0);
    }
}

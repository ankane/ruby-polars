use std::sync::Arc;

use magnus::{Error, Ruby, Value, gc, value::Opaque};
use polars::error::PolarsError;

use crate::ruby::gvl::GvlExt;

pub(crate) fn to_pl_err(e: Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

#[derive(Clone)]
pub struct ArcValue(pub Arc<Opaque<Value>>);

impl ArcValue {
    pub fn new(value: Opaque<Value>) -> Self {
        let ob = Arc::new(value);
        gc::register_address(&*ob);
        Self(ob)
    }
}

impl Drop for ArcValue {
    fn drop(&mut self) {
        // TODO use rb.gc_register_address
        Ruby::attach(|_| gc::unregister_address(&*self.0));
    }
}

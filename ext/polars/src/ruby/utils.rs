use magnus::{Error, Value, gc, value::Opaque};
use polars::error::PolarsError;

pub(crate) fn to_pl_err(e: Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

pub struct BoxOpaque(pub Box<Opaque<Value>>);

impl BoxOpaque {
    pub fn new(v: Value) -> Self {
        let boxed = Box::new(Opaque::from(v));
        gc::register_address(&*boxed);
        Self(boxed)
    }
}

impl Drop for BoxOpaque {
    fn drop(&mut self) {
        gc::unregister_address(&*self.0);
    }
}

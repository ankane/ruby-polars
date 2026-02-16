use magnus::{Error, Value, value::Opaque};
use polars::error::PolarsError;

pub(crate) fn to_pl_err(e: Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

pub(crate) struct BoxOpaque(pub(crate) Box<Opaque<Value>>);

impl BoxOpaque {
    pub(crate) fn new(v: Value) -> Self {
        let boxed = Box::new(Opaque::from(v));
        magnus::gc::register_address(&*boxed);
        Self(boxed)
    }
}

impl Drop for BoxOpaque {
    fn drop(&mut self) {
        magnus::gc::unregister_address(&*self.0);
    }
}

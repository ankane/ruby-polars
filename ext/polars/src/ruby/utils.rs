use std::sync::Arc;

use magnus::{Error, IntoValue, Ruby, Value, value::Opaque};
use polars::error::PolarsError;

use crate::RbResult;
use crate::ruby::gvl::GvlExt;
use crate::ruby::thread::is_non_ruby_thread;
use crate::ruby::thread::run_in_ruby_thread;

pub(crate) fn to_pl_err(e: Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

#[derive(Clone)]
pub struct ArcValue(pub Arc<Opaque<Value>>);

impl ArcValue {
    pub fn new(value: Value) -> Self {
        let arc = Arc::new(Opaque::from(value));
        Ruby::get_with(value).gc_register_address(&*arc);
        Self(arc)
    }
}

impl Drop for ArcValue {
    fn drop(&mut self) {
        if is_non_ruby_thread() {
            println!("need to queue address!");
            let arc = self.0.clone();
            run_in_ruby_thread(move || Ruby::attach(|rb| rb.gc_unregister_address(&*arc)));
            return;
        }
        Ruby::attach(|rb| rb.gc_unregister_address(&*self.0));
    }
}

pub trait TryIntoValue {
    fn try_into_value_with(self, ruby: &Ruby) -> RbResult<Value>;
}

impl<T: IntoValue> TryIntoValue for T {
    fn try_into_value_with(self, ruby: &Ruby) -> RbResult<Value> {
        Ok(self.into_value_with(ruby))
    }
}

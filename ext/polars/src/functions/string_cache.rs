use crate::RbResult;
use magnus::{RArray, Ruby, Value};
use polars_core::StringCacheHolder;

pub fn enable_string_cache() {
    polars_core::enable_string_cache()
}

pub fn disable_string_cache() {
    polars_core::disable_string_cache()
}

pub fn using_string_cache() -> bool {
    polars_core::using_string_cache()
}

#[magnus::wrap(class = "Polars::RbStringCacheHolder")]
pub struct RbStringCacheHolder {}

impl RbStringCacheHolder {
    pub fn hold() -> RbResult<Value> {
        let _hold = StringCacheHolder::hold();
        Ruby::get().unwrap().yield_splat(RArray::new())
    }
}

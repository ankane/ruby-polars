use crate::RbResult;
use magnus::{RArray, Ruby, Value};

pub fn enable_string_cache() {
    // The string cache no longer exists.
}

pub fn disable_string_cache() {
    // The string cache no longer exists.
}

pub fn using_string_cache() -> bool {
    // The string cache no longer exists.
    true
}

#[magnus::wrap(class = "Polars::RbStringCacheHolder")]
pub struct RbStringCacheHolder {}

impl RbStringCacheHolder {
    pub fn hold() -> RbResult<Value> {
        Ruby::get().unwrap().yield_splat(RArray::new())
    }
}

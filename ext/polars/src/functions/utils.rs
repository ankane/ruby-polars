use crate::RbResult;
use polars_core::config::get_engine_affinity;

pub fn rb_get_engine_affinity() -> RbResult<String> {
    Ok(get_engine_affinity())
}

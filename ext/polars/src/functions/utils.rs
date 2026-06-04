use crate::RbResult;

pub fn rb_get_engine_affinity() -> RbResult<String> {
    Ok(polars_config::config()
        .engine_affinity()
        .as_static_str()
        .to_string())
}

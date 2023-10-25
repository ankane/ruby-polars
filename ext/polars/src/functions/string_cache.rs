pub fn enable_string_cache() {
    polars_core::enable_string_cache()
}

pub fn disable_string_cache() {
    polars_core::disable_string_cache()
}

pub fn using_string_cache() -> bool {
    polars_core::using_string_cache()
}

use crate::RbResult;

pub fn escape_regex(s: String) -> RbResult<String> {
    let escaped_s = polars_ops::chunked_array::strings::escape_regex_str(&s);
    Ok(escaped_s)
}

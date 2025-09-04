use magnus::{Value, prelude::*};
use polars::prelude::*;

use crate::rb_modules::utils;

pub fn datetime_to_rb_object(v: i64, tu: TimeUnit, tz: Option<&TimeZone>) -> Value {
    let tu = tu.to_ascii();
    utils()
        .funcall("_to_ruby_datetime", (v, tu, tz.map(|v| v.to_string())))
        .unwrap()
}

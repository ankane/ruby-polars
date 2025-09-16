use magnus::{Value, prelude::*};
use polars::prelude::*;

use crate::rb_modules::pl_utils;

pub fn datetime_to_rb_object(v: i64, tu: TimeUnit, tz: Option<&TimeZone>) -> Value {
    let tu = tu.to_ascii();
    pl_utils()
        .funcall("_to_ruby_datetime", (v, tu, tz.map(|v| v.to_string())))
        .unwrap()
}

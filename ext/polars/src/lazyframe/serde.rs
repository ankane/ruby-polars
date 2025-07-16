use magnus::Value;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use std::io::Read;

use crate::file::get_file_like;
use crate::{RbLazyFrame, RbResult, RbValueError};

impl RbLazyFrame {
    // TODO change to serialize_json
    pub fn read_json(rb_f: Value) -> RbResult<Self> {
        // it is faster to first read to memory and then parse: https://github.com/serde-rs/json/issues/160
        // so don't bother with files.
        let mut json = String::new();
        let _ = get_file_like(rb_f, false)?
            .read_to_string(&mut json)
            .unwrap();

        // Safety
        // we skipped the serializing/deserializing of the static in lifetime in `DataType`
        // so we actually don't have a lifetime at all when serializing.

        // &str still has a lifetime. Bit its ok, because we drop it immediately
        // in this scope
        let json = unsafe { std::mem::transmute::<&'_ str, &'static str>(json.as_str()) };

        let lp = serde_json::from_str::<DslPlan>(json)
            .map_err(|err| RbValueError::new_err(format!("{err:?}")))?;
        Ok(LazyFrame::from(lp).into())
    }
}

#[cfg(feature = "serialize_binary")]
use std::io::{BufReader, BufWriter};
use std::io::{BufWriter, Read};

use magnus::Value;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;

use crate::exceptions::ComputeError;
use crate::file::get_file_like;
#[cfg(feature = "serialize_binary")]
use crate::utils::to_rb_err;
use crate::{RbLazyFrame, RbResult, RbValueError};

impl RbLazyFrame {
    #[cfg(feature = "serialize_binary")]
    pub fn serialize_binary(&self, rb_f: Value) -> RbResult<()> {
        let file = get_file_like(rb_f, true)?;
        let writer = BufWriter::new(file);
        self.ldf
            .read()
            .logical_plan
            .serialize_versioned(writer, Default::default())
            .map_err(to_rb_err)
    }

    pub fn serialize_json(&self, rb_f: Value) -> RbResult<()> {
        let file = get_file_like(rb_f, true)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &self.ldf.read().logical_plan)
            .map_err(|err| ComputeError::new_err(err.to_string()))
    }

    #[cfg(feature = "serialize_binary")]
    pub fn deserialize_binary(rb_f: Value) -> RbResult<Self> {
        let file = get_file_like(rb_f, false)?;
        let reader = BufReader::new(file);

        let lp: DslPlan = DslPlan::deserialize_versioned(reader).map_err(to_rb_err)?;
        Ok(LazyFrame::from(lp).into())
    }

    pub fn deserialize_json(rb_f: Value) -> RbResult<Self> {
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

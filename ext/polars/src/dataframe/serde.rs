use std::io::{BufReader, BufWriter};

use polars::prelude::*;

use crate::file::get_file_like;
use crate::utils::to_rb_err;
use crate::{RbDataFrame, RbPolarsErr, RbResult};
use magnus::Value;

impl RbDataFrame {
    pub fn serialize_binary(&self, rb_f: Value) -> RbResult<()> {
        let file = get_file_like(rb_f, true)?;
        let mut writer = BufWriter::new(file);

        Ok(self
            .df
            .borrow_mut()
            .serialize_into_writer(&mut writer)
            .map_err(RbPolarsErr::from)?)
    }

    pub fn deserialize_binary(rb_f: Value) -> RbResult<Self> {
        let file = get_file_like(rb_f, false)?;
        let mut file = BufReader::new(file);

        DataFrame::deserialize_from_reader(&mut file)
            .map(|v| v.into())
            .map_err(to_rb_err)
    }
}

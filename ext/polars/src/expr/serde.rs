use std::io::{BufReader, BufWriter};

use magnus::Value;
use polars::lazy::prelude::Expr;
use polars_utils::pl_serialize;

use crate::exceptions::ComputeError;
use crate::file::get_file_like;
use crate::{RbExpr, RbResult};

impl RbExpr {
    pub fn serialize_binary(&self, rb_f: Value) -> RbResult<()> {
        let file = get_file_like(rb_f, true)?;
        let writer = BufWriter::new(file);
        pl_serialize::SerializeOptions::default()
            .serialize_into_writer::<_, _, true>(writer, &self.inner)
            .map_err(|err| ComputeError::new_err(err.to_string()))
    }

    pub fn deserialize_binary(rb_f: Value) -> RbResult<RbExpr> {
        let file = get_file_like(rb_f, false)?;
        let reader = BufReader::new(file);
        let expr: Expr = pl_serialize::SerializeOptions::default()
            .deserialize_from_reader::<_, _, true>(reader)
            .map_err(|err| ComputeError::new_err(err.to_string()))?;
        Ok(expr.into())
    }
}

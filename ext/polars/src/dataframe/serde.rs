use crate::error::ComputeError;
use crate::file::get_file_like;
use crate::{RbDataFrame, RbResult};
use magnus::Value;
use std::io::BufWriter;

impl RbDataFrame {
    // TODO add to Ruby
    pub fn serialize_json(&self, rb_f: Value) -> RbResult<()> {
        let file = get_file_like(rb_f, true)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, &self.df)
            .map_err(|err| ComputeError::new_err(err.to_string()))
    }
}

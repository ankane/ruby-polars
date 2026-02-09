use magnus::{TryConvert, Value};
use polars::prelude::SinkDestination;

use crate::RbResult;

pub struct RbFileSinkDestination(Value);

impl TryConvert for RbFileSinkDestination {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self(ob))
    }
}

impl RbFileSinkDestination {
    pub fn extract_file_sink_destination(&self) -> RbResult<SinkDestination> {
        todo!();
    }
}

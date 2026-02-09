use magnus::{TryConvert, Value};
use polars::prelude::SinkDestination;

use crate::RbResult;
use crate::prelude::Wrap;

pub struct RbFileSinkDestination(Value);

impl TryConvert for RbFileSinkDestination {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self(ob))
    }
}

impl RbFileSinkDestination {
    pub fn extract_file_sink_destination(&self) -> RbResult<SinkDestination> {
        let v = Wrap::<polars_plan::dsl::SinkTarget>::try_convert(self.0)?;

        Ok(SinkDestination::File { target: v.0 })
    }
}

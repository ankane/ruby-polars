use magnus::{TryConvert, Value};
use polars::prelude::{CloudScheme, UnifiedSinkArgs};

use crate::RbResult;

/// Interface to `class SinkOptions` on the Ruby side
pub struct RbSinkOptions(Value);

impl TryConvert for RbSinkOptions {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self(ob))
    }
}

impl RbSinkOptions {
    pub fn extract_unified_sink_args(
        &self,
        cloud_scheme: Option<CloudScheme>,
    ) -> RbResult<UnifiedSinkArgs> {
        todo!();
    }
}

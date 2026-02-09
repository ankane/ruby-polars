use magnus::{RHash, TryConvert, Value};
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars::prelude::{CloudScheme, UnifiedSinkArgs};

use crate::{RbResult, RbValueError, Wrap};

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
        if cloud_scheme.is_some() {
            todo!();
        }

        let parsed = RHash::try_convert(self.0)?;

        if parsed.len() != 3 {
            return Err(RbValueError::new_err(
                "`sink_options` must be a hash with the exactly 3 field.",
            ));
        }

        let sync_on_close = parsed.get("sync_on_close").ok_or_else(|| {
            RbValueError::new_err("`sink_options` must contain `sync_on_close` field")
        })?;
        let sync_on_close = Wrap::<SyncOnCloseType>::try_convert(sync_on_close)?.0;

        let maintain_order = parsed.get("maintain_order").ok_or_else(|| {
            RbValueError::new_err("`sink_options` must contain `maintain_order` field")
        })?;
        let maintain_order = bool::try_convert(maintain_order)?;

        let mkdir = parsed
            .get("mkdir")
            .ok_or_else(|| RbValueError::new_err("`sink_options` must contain `mkdir` field"))?;
        let mkdir = bool::try_convert(mkdir)?;

        let unified_sink_args = UnifiedSinkArgs {
            mkdir,
            maintain_order,
            sync_on_close,
            // TODO fix
            cloud_options: None, //cloud_options.map(Arc::new),
        };

        Ok(unified_sink_args)
    }
}

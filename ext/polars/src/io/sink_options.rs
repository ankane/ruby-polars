use std::sync::Arc;

use magnus::{TryConvert, Value, value::ReprValue};
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars::prelude::{CloudScheme, UnifiedSinkArgs};

use crate::io::cloud_options::OptRbCloudOptions;
use crate::{RbResult, Wrap};

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
        let mkdir: bool = self.0.funcall("mkdir", ())?;
        let maintain_order: bool = self.0.funcall("maintain_order", ())?;
        let sync_on_close: Option<Wrap<SyncOnCloseType>> = self.0.funcall("sync_on_close", ())?;
        let storage_options: OptRbCloudOptions = self.0.funcall("storage_options", ())?;
        let credential_provider: Option<Value> = self.0.funcall("credential_provider", ())?;

        let cloud_options =
            storage_options.extract_opt_cloud_options(cloud_scheme, credential_provider)?;

        let sync_on_close = sync_on_close.map_or(SyncOnCloseType::default(), |x| x.0);

        let unified_sink_args = UnifiedSinkArgs {
            mkdir,
            maintain_order,
            sync_on_close,
            cloud_options: cloud_options.map(Arc::new),
        };

        Ok(unified_sink_args)
    }
}

use magnus::{TryConvert, Value, value::ReprValue};
use polars::prelude::CloudScheme;
use polars_io::cloud::CloudOptions;

use crate::RbResult;

pub struct OptRbCloudOptions(Value);

impl TryConvert for OptRbCloudOptions {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self(ob))
    }
}

impl OptRbCloudOptions {
    pub fn extract_opt_cloud_options(
        &self,
        _cloud_scheme: Option<CloudScheme>,
        credential_provider: Option<Value>,
    ) -> RbResult<Option<CloudOptions>> {
        if self.0.is_nil() && credential_provider.is_none() {
            return Ok(None);
        }

        todo!();
    }
}

use std::path::{Path, PathBuf};
use std::sync::Arc;

use magnus::{RHash, TryConvert, Value};
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars::prelude::SinkOptions;

use crate::prelude::Wrap;
use crate::{RbResult, RbValueError};

#[derive(Clone)]
pub enum SinkTarget {
    File(polars_plan::dsl::SinkTarget),
}

impl TryConvert for Wrap<polars_plan::dsl::SinkTarget> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        if let Ok(v) = PathBuf::try_convert(ob) {
            Ok(Wrap(polars::prelude::SinkTarget::Path(Arc::new(v))))
        } else {
            todo!();
        }
    }
}

impl TryConvert for SinkTarget {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self::File(
            <Wrap<polars_plan::dsl::SinkTarget>>::try_convert(ob)?.0,
        ))
    }
}

impl SinkTarget {
    pub fn base_path(&self) -> Option<&Path> {
        match self {
            Self::File(t) => match t {
                polars::prelude::SinkTarget::Path(p) => Some(p.as_path()),
                polars::prelude::SinkTarget::Dyn(_) => None,
            },
        }
    }
}

impl TryConvert for Wrap<SyncOnCloseType> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = match String::try_convert(ob)?.as_str() {
            "none" => SyncOnCloseType::None,
            "data" => SyncOnCloseType::Data,
            "all" => SyncOnCloseType::All,
            v => {
                return Err(RbValueError::new_err(format!(
                    "`sync_on_close` must be one of {{'none', 'data', 'all'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl TryConvert for Wrap<SinkOptions> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = RHash::try_convert(ob)?;

        if parsed.len() != 3 {
            return Err(RbValueError::new_err(
                "`sink_options` must be a dictionary with the exactly 3 field.",
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

        Ok(Wrap(SinkOptions {
            sync_on_close,
            maintain_order,
            mkdir,
        }))
    }
}

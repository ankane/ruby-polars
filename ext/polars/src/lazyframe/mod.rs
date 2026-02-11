mod exitable;
mod general;
mod optflags;
mod serde;
mod sink;

pub use exitable::RbInProcessQuery;
pub use general::RbCollectBatches;
use magnus::{TryConvert, Value};
use parking_lot::RwLock;
use polars::prelude::{Engine, LazyFrame, OptFlags};

use crate::prelude::Wrap;
use crate::{RbResult, RbValueError};

#[magnus::wrap(class = "Polars::RbLazyFrame")]
#[repr(transparent)]
pub struct RbLazyFrame {
    pub ldf: RwLock<LazyFrame>,
}

impl Clone for RbLazyFrame {
    fn clone(&self) -> Self {
        Self {
            ldf: RwLock::new(self.ldf.read().clone()),
        }
    }
}

impl From<LazyFrame> for RbLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        RbLazyFrame {
            ldf: RwLock::new(ldf),
        }
    }
}

impl From<RbLazyFrame> for LazyFrame {
    fn from(pldf: RbLazyFrame) -> Self {
        pldf.ldf.into_inner()
    }
}

#[magnus::wrap(class = "Polars::RbOptFlags")]
pub struct RbOptFlags {
    pub inner: RwLock<OptFlags>,
}

impl Clone for RbOptFlags {
    fn clone(&self) -> Self {
        Self {
            inner: RwLock::new(*self.inner.read()),
        }
    }
}

impl From<OptFlags> for RbOptFlags {
    fn from(inner: OptFlags) -> Self {
        RbOptFlags {
            inner: RwLock::new(inner),
        }
    }
}

impl TryConvert for Wrap<Engine> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let parsed = String::try_convert(ob)?
            .parse()
            .map_err(RbValueError::new_err)?;
        Ok(Wrap(parsed))
    }
}

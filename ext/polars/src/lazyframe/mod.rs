mod general;
mod optflags;
mod serde;
mod sink;

use magnus::{TryConvert, Value};
use polars::prelude::{Engine, LazyFrame, OptFlags};
pub use sink::SinkTarget;
use std::cell::RefCell;

use crate::prelude::Wrap;
use crate::{RbResult, RbValueError};

#[magnus::wrap(class = "Polars::RbLazyFrame")]
#[derive(Clone)]
pub struct RbLazyFrame {
    pub ldf: RefCell<LazyFrame>,
}

#[magnus::wrap(class = "Polars::RbOptFlags")]
#[derive(Clone)]
pub struct RbOptFlags {
    pub inner: RefCell<OptFlags>,
}

impl From<LazyFrame> for RbLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        RbLazyFrame {
            ldf: RefCell::new(ldf),
        }
    }
}

impl From<OptFlags> for RbOptFlags {
    fn from(inner: OptFlags) -> Self {
        RbOptFlags {
            inner: RefCell::new(inner),
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

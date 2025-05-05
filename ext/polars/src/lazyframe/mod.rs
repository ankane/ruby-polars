mod general;
mod serde;
mod sink;

use polars::lazy::frame::LazyFrame;
pub use sink::SinkTarget;
use std::cell::RefCell;

#[magnus::wrap(class = "Polars::RbLazyFrame")]
#[derive(Clone)]
pub struct RbLazyFrame {
    pub ldf: RefCell<LazyFrame>,
}

impl From<LazyFrame> for RbLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        RbLazyFrame {
            ldf: RefCell::new(ldf),
        }
    }
}

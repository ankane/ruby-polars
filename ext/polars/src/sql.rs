use polars::sql::SQLContext;
use std::cell::RefCell;

use crate::{RbLazyFrame, RbPolarsErr, RbResult};

#[magnus::wrap(class = "Polars::RbSQLContext")]
#[repr(transparent)]
#[derive(Clone)]
pub struct RbSQLContext {
    pub context: RefCell<SQLContext>,
}

#[allow(
    clippy::wrong_self_convention,
    clippy::should_implement_trait,
    clippy::len_without_is_empty
)]
impl RbSQLContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> RbSQLContext {
        RbSQLContext {
            context: SQLContext::new().into(),
        }
    }

    pub fn execute(&self, query: String) -> RbResult<RbLazyFrame> {
        Ok(self
            .context
            .borrow_mut()
            .execute(&query)
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn get_tables(&self) -> RbResult<Vec<String>> {
        Ok(self.context.borrow().get_tables())
    }

    pub fn register(&self, name: String, lf: &RbLazyFrame) {
        self.context
            .borrow_mut()
            .register(&name, lf.ldf.borrow().clone())
    }

    pub fn unregister(&self, name: String) {
        self.context.borrow_mut().unregister(&name)
    }
}

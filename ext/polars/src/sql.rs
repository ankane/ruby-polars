use parking_lot::RwLock;
use polars::sql::SQLContext;

use crate::{RbLazyFrame, RbPolarsErr, RbResult};

#[magnus::wrap(class = "Polars::RbSQLContext")]
#[repr(transparent)]
pub struct RbSQLContext {
    pub context: RwLock<SQLContext>,
}

impl Clone for RbSQLContext {
    fn clone(&self) -> Self {
        Self {
            context: RwLock::new(self.context.read().clone()),
        }
    }
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
            context: RwLock::new(SQLContext::new()),
        }
    }

    pub fn execute(&self, query: String) -> RbResult<RbLazyFrame> {
        Ok(self
            .context
            .write()
            .execute(&query)
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn get_tables(&self) -> RbResult<Vec<String>> {
        Ok(self.context.read().get_tables())
    }

    pub fn register(&self, name: String, lf: &RbLazyFrame) {
        self.context.write().register(&name, lf.ldf.read().clone())
    }

    pub fn unregister(&self, name: String) {
        self.context.write().unregister(&name)
    }
}

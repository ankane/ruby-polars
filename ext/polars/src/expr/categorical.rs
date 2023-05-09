use polars::prelude::*;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn cat_set_ordering(&self, ordering: Wrap<CategoricalOrdering>) -> Self {
        self.inner.clone().cat().set_ordering(ordering.0).into()
    }
}

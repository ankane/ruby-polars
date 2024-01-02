use polars::prelude::*;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn cat_set_ordering(&self, ordering: Wrap<CategoricalOrdering>) -> Self {
        self.inner
            .clone()
            .cast(DataType::Categorical(None, ordering.0))
            .into()
    }

    pub fn cat_get_categories(&self) -> Self {
        self.inner.clone().cat().get_categories().into()
    }
}

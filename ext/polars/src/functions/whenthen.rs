use polars::lazy::dsl;

use crate::RbExpr;

#[magnus::wrap(class = "Polars::RbWhen")]
#[derive(Clone)]
pub struct RbWhen {
    pub inner: dsl::When,
}

impl From<dsl::When> for RbWhen {
    fn from(inner: dsl::When) -> Self {
        RbWhen { inner }
    }
}

#[magnus::wrap(class = "Polars::RbWhenThen")]
#[derive(Clone)]
pub struct RbWhenThen {
    pub inner: dsl::WhenThen,
}

impl From<dsl::WhenThen> for RbWhenThen {
    fn from(inner: dsl::WhenThen) -> Self {
        RbWhenThen { inner }
    }
}

impl RbWhen {
    pub fn then(&self, expr: &RbExpr) -> RbWhenThen {
        self.inner.clone().then(expr.inner.clone()).into()
    }
}

impl RbWhenThen {
    pub fn overwise(&self, expr: &RbExpr) -> RbExpr {
        self.inner.clone().otherwise(expr.inner.clone()).into()
    }
}

pub fn when(predicate: &RbExpr) -> RbWhen {
    dsl::when(predicate.inner.clone()).into()
}

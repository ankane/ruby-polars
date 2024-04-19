use polars::lazy::dsl;

use crate::RbExpr;

pub fn when(condition: &RbExpr) -> RbWhen {
    RbWhen {
        inner: dsl::when(condition.inner.clone()),
    }
}

#[magnus::wrap(class = "Polars::RbWhen")]
#[derive(Clone)]
pub struct RbWhen {
    pub inner: dsl::When,
}

#[magnus::wrap(class = "Polars::RbThen")]
#[derive(Clone)]
pub struct RbThen {
    pub inner: dsl::Then,
}

#[magnus::wrap(class = "Polars::RbChainedWhen")]
#[derive(Clone)]
pub struct RbChainedWhen {
    pub inner: dsl::ChainedWhen,
}

#[magnus::wrap(class = "Polars::RbChainedThen")]
#[derive(Clone)]
pub struct RbChainedThen {
    pub inner: dsl::ChainedThen,
}

impl RbWhen {
    pub fn then(&self, statement: &RbExpr) -> RbThen {
        RbThen {
            inner: self.inner.clone().then(statement.inner.clone()),
        }
    }
}

impl RbThen {
    pub fn when(&self, condition: &RbExpr) -> RbChainedWhen {
        RbChainedWhen {
            inner: self.inner.clone().when(condition.inner.clone()),
        }
    }

    pub fn otherwise(&self, statement: &RbExpr) -> RbExpr {
        self.inner.clone().otherwise(statement.inner.clone()).into()
    }
}

impl RbChainedWhen {
    pub fn then(&self, statement: &RbExpr) -> RbChainedThen {
        RbChainedThen {
            inner: self.inner.clone().then(statement.inner.clone()),
        }
    }
}

impl RbChainedThen {
    pub fn when(&self, condition: &RbExpr) -> RbChainedWhen {
        RbChainedWhen {
            inner: self.inner.clone().when(condition.inner.clone()),
        }
    }

    pub fn otherwise(&self, statement: &RbExpr) -> RbExpr {
        self.inner.clone().otherwise(statement.inner.clone()).into()
    }
}

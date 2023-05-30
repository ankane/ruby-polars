use crate::RbExpr;

impl RbExpr {
    pub fn array_max(&self) -> Self {
        self.inner.clone().arr().max().into()
    }

    pub fn array_min(&self) -> Self {
        self.inner.clone().arr().min().into()
    }

    pub fn array_sum(&self) -> Self {
        self.inner.clone().arr().sum().into()
    }
}

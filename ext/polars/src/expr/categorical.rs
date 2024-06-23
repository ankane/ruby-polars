use crate::RbExpr;

impl RbExpr {
    pub fn cat_get_categories(&self) -> Self {
        self.inner.clone().cat().get_categories().into()
    }
}

use crate::RbExpr;

impl RbExpr {
    pub fn str_concat(&self, delimiter: String) -> Self {
        self.inner.clone().str().concat(&delimiter).into()
    }
}

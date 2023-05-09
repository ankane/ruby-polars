use crate::RbExpr;

impl RbExpr {
    pub fn bin_contains(&self, lit: Vec<u8>) -> Self {
        self.inner.clone().binary().contains_literal(lit).into()
    }

    pub fn bin_ends_with(&self, sub: Vec<u8>) -> Self {
        self.inner.clone().binary().ends_with(sub).into()
    }

    pub fn bin_starts_with(&self, sub: Vec<u8>) -> Self {
        self.inner.clone().binary().starts_with(sub).into()
    }
}

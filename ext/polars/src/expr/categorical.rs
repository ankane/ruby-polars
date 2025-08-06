use crate::RbExpr;

impl RbExpr {
    pub fn cat_get_categories(&self) -> Self {
        self.inner.clone().cat().get_categories().into()
    }

    pub fn cat_len_bytes(&self) -> Self {
        self.inner.clone().cat().len_bytes().into()
    }

    pub fn cat_len_chars(&self) -> Self {
        self.inner.clone().cat().len_chars().into()
    }

    pub fn cat_starts_with(&self, prefix: String) -> Self {
        self.inner.clone().cat().starts_with(prefix).into()
    }

    pub fn cat_ends_with(&self, suffix: String) -> Self {
        self.inner.clone().cat().ends_with(suffix).into()
    }

    pub fn cat_slice(&self, offset: i64, length: Option<usize>) -> Self {
        self.inner.clone().cat().slice(offset, length).into()
    }
}

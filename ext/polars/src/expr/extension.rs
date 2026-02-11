use crate::RbExpr;
use crate::expr::datatype::RbDataTypeExpr;

impl RbExpr {
    pub fn ext_storage(&self) -> Self {
        self.inner.clone().ext().storage().into()
    }

    pub fn ext_to(&self, dtype: &RbDataTypeExpr) -> Self {
        self.inner.clone().ext().to(dtype.inner.clone()).into()
    }
}

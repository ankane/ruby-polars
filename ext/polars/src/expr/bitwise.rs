use crate::RbExpr;

impl RbExpr {
    pub fn bitwise_count_ones(&self) -> Self {
        self.inner.clone().bitwise_count_ones().into()
    }

    pub fn bitwise_count_zeros(&self) -> Self {
        self.inner.clone().bitwise_count_zeros().into()
    }

    pub fn bitwise_leading_ones(&self) -> Self {
        self.inner.clone().bitwise_leading_ones().into()
    }

    pub fn bitwise_leading_zeros(&self) -> Self {
        self.inner.clone().bitwise_leading_zeros().into()
    }

    pub fn bitwise_trailing_ones(&self) -> Self {
        self.inner.clone().bitwise_trailing_ones().into()
    }

    pub fn bitwise_trailing_zeros(&self) -> Self {
        self.inner.clone().bitwise_trailing_zeros().into()
    }

    pub fn bitwise_and(&self) -> Self {
        self.inner.clone().bitwise_and().into()
    }

    pub fn bitwise_or(&self) -> Self {
        self.inner.clone().bitwise_or().into()
    }

    pub fn bitwise_xor(&self) -> Self {
        self.inner.clone().bitwise_xor().into()
    }
}

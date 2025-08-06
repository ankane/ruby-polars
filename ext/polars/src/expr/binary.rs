use crate::expr::datatype::RbDataTypeExpr;
use crate::{RbExpr, RbResult};

impl RbExpr {
    pub fn bin_contains(&self, lit: &RbExpr) -> Self {
        self.inner
            .clone()
            .binary()
            .contains_literal(lit.inner.clone())
            .into()
    }

    pub fn bin_ends_with(&self, sub: &RbExpr) -> Self {
        self.inner
            .clone()
            .binary()
            .ends_with(sub.inner.clone())
            .into()
    }

    pub fn bin_starts_with(&self, sub: &RbExpr) -> Self {
        self.inner
            .clone()
            .binary()
            .starts_with(sub.inner.clone())
            .into()
    }

    pub fn bin_hex_decode(&self, strict: bool) -> Self {
        self.inner.clone().binary().hex_decode(strict).into()
    }

    pub fn bin_base64_decode(&self, strict: bool) -> Self {
        self.inner.clone().binary().base64_decode(strict).into()
    }

    pub fn bin_hex_encode(&self) -> Self {
        self.inner.clone().binary().hex_encode().into()
    }

    pub fn bin_base64_encode(&self) -> Self {
        self.inner.clone().binary().base64_encode().into()
    }

    pub fn bin_reinterpret(&self, dtype: &RbDataTypeExpr, kind: String) -> RbResult<Self> {
        use crate::RbValueError;

        let is_little_endian = match kind.to_lowercase().as_str() {
            "little" => true,
            "big" => false,
            _ => {
                return Err(RbValueError::new_err(format!(
                    "Invalid endianness: {kind}. Valid values are \"little\" or \"big\"."
                )));
            }
        };
        Ok(self
            .inner
            .clone()
            .binary()
            .reinterpret(dtype.inner.clone(), is_little_endian)
            .into())
    }

    pub fn bin_size_bytes(&self) -> Self {
        self.inner.clone().binary().size_bytes().into()
    }
}

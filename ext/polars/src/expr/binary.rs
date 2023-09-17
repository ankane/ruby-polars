use polars::prelude::*;

use crate::RbExpr;

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
        self.clone()
            .inner
            .map(
                move |s| {
                    s.binary()?
                        .hex_decode(strict)
                        .map(|s| Some(s.into_series()))
                },
                GetOutput::same_type(),
            )
            .with_fmt("bin.hex_decode")
            .into()
    }

    pub fn bin_base64_decode(&self, strict: bool) -> Self {
        self.clone()
            .inner
            .map(
                move |s| {
                    s.binary()?
                        .base64_decode(strict)
                        .map(|s| Some(s.into_series()))
                },
                GetOutput::same_type(),
            )
            .with_fmt("bin.base64_decode")
            .into()
    }

    pub fn bin_hex_encode(&self) -> Self {
        self.clone()
            .inner
            .map(
                move |s| s.binary().map(|s| Some(s.hex_encode().into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("bin.hex_encode")
            .into()
    }

    pub fn bin_base64_encode(&self) -> Self {
        self.clone()
            .inner
            .map(
                move |s| s.binary().map(|s| Some(s.base64_encode().into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("bin.base64_encode")
            .into()
    }
}

use polars::lazy::dsl::Expr;

#[magnus::wrap(class = "Polars::RbExpr")]
#[derive(Clone)]
pub struct RbExpr {
    pub inner: Expr,
}

impl From<Expr> for RbExpr {
    fn from(inner: Expr) -> Self {
        RbExpr { inner }
    }
}

use polars::prelude::DataTypeExpr;

#[magnus::wrap(class = "Polars::RbDataTypeExpr")]
#[repr(transparent)]
#[derive(Clone)]
pub struct RbDataTypeExpr {
    pub inner: DataTypeExpr,
}

impl From<DataTypeExpr> for RbDataTypeExpr {
    fn from(expr: DataTypeExpr) -> Self {
        RbDataTypeExpr { inner: expr }
    }
}

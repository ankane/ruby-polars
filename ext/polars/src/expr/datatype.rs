use magnus::{IntoValue, Ruby, Value};
use polars::prelude::{DataType, DataTypeExpr, Schema};

use crate::prelude::Wrap;
use crate::{RbExpr, RbPolarsErr, RbResult};

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

impl RbDataTypeExpr {
    pub fn from_dtype(datatype: Wrap<DataType>) -> Self {
        DataTypeExpr::Literal(datatype.0).into()
    }

    pub fn of_expr(expr: &RbExpr) -> Self {
        DataTypeExpr::OfExpr(Box::new(expr.inner.clone())).into()
    }

    pub fn collect_dtype(ruby: &Ruby, rb_self: &Self, schema: Wrap<Schema>) -> RbResult<Value> {
        let dtype = rb_self
            .clone()
            .inner
            .into_datatype(&schema.0)
            .map_err(RbPolarsErr::from)?;
        Ok(Wrap(dtype).into_value_with(ruby))
    }
}

use magnus::{RArray, Ruby, TryConvert, Value};
use polars::prelude::{DataType, DataTypeExpr, PlSmallStr, Schema};

use crate::prelude::Wrap;
use crate::ruby::utils::TryIntoValue;
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

    pub fn self_dtype() -> Self {
        DataTypeExpr::SelfDtype.into()
    }

    pub fn collect_dtype(ruby: &Ruby, self_: &Self, schema: Wrap<Schema>) -> RbResult<Value> {
        let dtype = self_
            .clone()
            .inner
            .into_datatype(&schema.0)
            .map_err(RbPolarsErr::from)?;
        Wrap(dtype).try_into_value_with(ruby)
    }

    pub fn struct_with_fields(rb_fields: RArray) -> RbResult<Self> {
        let mut fields = Vec::new();
        for v in rb_fields.into_iter() {
            let (name, dt_expr) = <(String, &RbDataTypeExpr)>::try_convert(v)?;
            fields.push((PlSmallStr::from_string(name), dt_expr.inner.clone()));
        }
        Ok(DataTypeExpr::StructWithFields(fields).into())
    }
}

use crate::conversion::Wrap;
use crate::prelude::DataType;
use crate::RbResult;

pub fn dtype_str_repr(dtype: Wrap<DataType>) -> RbResult<String> {
    let dtype = dtype.0;
    Ok(dtype.to_string())
}

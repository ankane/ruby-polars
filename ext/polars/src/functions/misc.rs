use crate::RbResult;
use crate::conversion::Wrap;
use crate::prelude::DataType;

pub fn dtype_str_repr(dtype: Wrap<DataType>) -> RbResult<String> {
    let dtype = dtype.0;
    Ok(dtype.to_string())
}

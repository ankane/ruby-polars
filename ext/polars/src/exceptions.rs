use crate::rb_modules;
use magnus::{exception, Error};

macro_rules! create_exception {
    ($type:ty, $cls:expr) => {
        impl $type {
            pub fn new_err(message: String) -> Error {
                Error::new($cls, message)
            }
        }
    };
}

pub struct RbTypeError {}
pub struct RbValueError {}
pub struct RbOverflowError {}
pub struct ComputeError {}
pub struct InvalidOperationError {}

create_exception!(RbTypeError, exception::type_error());
create_exception!(RbValueError, exception::arg_error());
create_exception!(RbOverflowError, exception::range_error());
create_exception!(ComputeError, rb_modules::compute_error());
create_exception!(InvalidOperationError, rb_modules::invalid_operation_error());

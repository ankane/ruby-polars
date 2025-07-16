use crate::rb_modules;
use magnus::{Error, exception};
use std::borrow::Cow;

macro_rules! create_exception {
    ($type:ident, $cls:expr) => {
        pub struct $type {}

        impl $type {
            pub fn new_err<T>(message: T) -> Error
            where
                T: Into<Cow<'static, str>>,
            {
                Error::new($cls, message)
            }
        }
    };
}

create_exception!(RbTypeError, exception::type_error());
create_exception!(RbValueError, exception::arg_error());
create_exception!(RbOverflowError, exception::range_error());
create_exception!(ComputeError, rb_modules::compute_error());
create_exception!(InvalidOperationError, rb_modules::invalid_operation_error());

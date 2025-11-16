use crate::rb_modules;
use magnus::{Error, Ruby};
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

create_exception!(RbTypeError, Ruby::get().unwrap().exception_type_error());
create_exception!(RbValueError, Ruby::get().unwrap().exception_arg_error());
create_exception!(
    RbOverflowError,
    Ruby::get().unwrap().exception_range_error()
);
create_exception!(RbIndexError, Ruby::get().unwrap().exception_index_error());
create_exception!(AssertionError, rb_modules::assertion_error());
create_exception!(ColumnNotFoundError, rb_modules::column_not_found_error());
create_exception!(ComputeError, rb_modules::compute_error());
create_exception!(InvalidOperationError, rb_modules::invalid_operation_error());

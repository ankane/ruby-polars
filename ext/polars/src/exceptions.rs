use crate::rb_modules;
use magnus::{Error, Module, Ruby};
use std::borrow::Cow;

macro_rules! create_exception {
    ($type:ident) => {
        pub struct $type {}

        impl $type {
            pub fn new_err<T>(message: T) -> Error
            where
                T: Into<Cow<'static, str>>,
            {
                let cls = rb_modules::polars().const_get(stringify!($type)).unwrap();
                Error::new(cls, message)
            }
        }
    };

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

// Errors
create_exception!(AssertionError);
create_exception!(ColumnNotFoundError);
create_exception!(ComputeError);
create_exception!(DuplicateError);
create_exception!(InvalidOperationError);
create_exception!(NoDataError);
create_exception!(OutOfBoundsError);
create_exception!(SQLInterfaceError);
create_exception!(SQLSyntaxError);
create_exception!(SchemaError);
create_exception!(SchemaFieldNotFoundError);
create_exception!(ShapeError);
create_exception!(StringCacheMismatchError);
create_exception!(StructFieldNotFoundError);

// Ruby errors
create_exception!(RbTypeError, Ruby::get().unwrap().exception_type_error());
create_exception!(RbValueError, Ruby::get().unwrap().exception_arg_error());
create_exception!(
    RbOverflowError,
    Ruby::get().unwrap().exception_range_error()
);
create_exception!(RbIndexError, Ruby::get().unwrap().exception_index_error());

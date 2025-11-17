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
}

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

macro_rules! create_ruby_exception {
    ($type:ident, $cls:ident) => {
        pub struct $type {}

        impl $type {
            pub fn new_err<T>(message: T) -> Error
            where
                T: Into<Cow<'static, str>>,
            {
                let cls = Ruby::get().unwrap().$cls();
                Error::new(cls, message)
            }
        }
    };
}

create_ruby_exception!(RbTypeError, exception_type_error);
create_ruby_exception!(RbValueError, exception_arg_error);
create_ruby_exception!(RbOverflowError, exception_range_error);
create_ruby_exception!(RbIndexError, exception_index_error);

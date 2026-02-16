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
                let ruby = Ruby::get().unwrap();
                let cls = rb_modules::polars(&ruby)
                    .const_get(stringify!($type))
                    .unwrap();
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

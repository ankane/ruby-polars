use magnus::{Error, Ruby};
use std::borrow::Cow;

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

create_ruby_exception!(RbKeyboardInterrupt, exception_interrupt);
create_ruby_exception!(RbIndexError, exception_index_error);
create_ruby_exception!(RbIOError, exception_io_error);
create_ruby_exception!(RbOverflowError, exception_range_error);
create_ruby_exception!(RbRuntimeError, exception_runtime_error);
create_ruby_exception!(RbTypeError, exception_type_error);
create_ruby_exception!(RbValueError, exception_arg_error);

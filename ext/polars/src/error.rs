use magnus::exception;
use magnus::Error;
use polars::prelude::PolarsError;

use crate::rb_modules;

pub struct RbPolarsErr {}

impl RbPolarsErr {
    // convert to Error instead of Self
    pub fn from(e: PolarsError) -> Error {
        Error::new(rb_modules::error(), e.to_string())
    }

    pub fn io(e: std::io::Error) -> Error {
        Error::new(rb_modules::error(), e.to_string())
    }

    pub fn other(message: String) -> Error {
        Error::new(rb_modules::error(), message)
    }
}

pub struct RbTypeError {}

impl RbTypeError {
    pub fn new_err(message: String) -> Error {
        Error::new(exception::type_error(), message)
    }
}

pub struct RbValueError {}

impl RbValueError {
    pub fn new_err(message: String) -> Error {
        Error::new(exception::arg_error(), message)
    }
}

pub struct ComputeError {}

impl ComputeError {
    pub fn new_err(message: String) -> Error {
        Error::new(exception::runtime_error(), message)
    }
}

pub struct RbOverflowError {}

impl RbOverflowError {
    pub fn new_err(message: String) -> Error {
        Error::new(exception::runtime_error(), message)
    }
}

#[macro_export]
macro_rules! raise_err(
    ($msg:expr, $err:ident) => {{
        Err(PolarsError::$err($msg.into())).map_err(RbPolarsErr::from)?;
        unreachable!()
    }}
);

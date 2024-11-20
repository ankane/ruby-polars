use magnus::exception;
use magnus::Error;
use polars::prelude::PolarsError;

use crate::rb_modules;

pub enum RbPolarsErr {
    // Polars(PolarsError),
    Other(String),
}

impl From<RbPolarsErr> for Error {
    fn from(err: RbPolarsErr) -> Self {
        match err {
            RbPolarsErr::Other(err) => Error::new(rb_modules::error(), err.to_string()),
        }
    }
}

impl RbPolarsErr {
    // convert to Error instead of Self
    pub fn from(e: PolarsError) -> Error {
        match e {
            PolarsError::ComputeError(err) => ComputeError::new_err(err.to_string()),
            PolarsError::InvalidOperation(err) => InvalidOperationError::new_err(err.to_string()),
            _ => Error::new(rb_modules::error(), e.to_string()),
        }
    }

    pub fn io(e: std::io::Error) -> Error {
        Error::new(rb_modules::error(), e.to_string())
    }
}

macro_rules! impl_error_class {
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

impl_error_class!(RbTypeError, exception::type_error());
impl_error_class!(RbValueError, exception::arg_error());
impl_error_class!(RbOverflowError, exception::range_error());
impl_error_class!(ComputeError, rb_modules::compute_error());
impl_error_class!(InvalidOperationError, rb_modules::invalid_operation_error());

#[macro_export]
macro_rules! raise_err(
    ($msg:expr, $err:ident) => {{
        Err(PolarsError::$err($msg.into())).map_err(RbPolarsErr::from)?;
        unreachable!()
    }}
);

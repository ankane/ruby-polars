use magnus::exception;
use magnus::Error;
use polars::error::ArrowError;
use polars::prelude::PolarsError;

pub struct RbPolarsErr {}

impl RbPolarsErr {
    // convert to Error instead of Self
    pub fn from(e: PolarsError) -> Error {
        Error::new(exception::runtime_error(), e.to_string())
    }

    pub fn arrow(e: ArrowError) -> Error {
        Error::new(exception::runtime_error(), e.to_string())
    }

    pub fn io(e: std::io::Error) -> Error {
        Error::new(exception::runtime_error(), e.to_string())
    }

    pub fn other(message: String) -> Error {
        Error::new(exception::runtime_error(), message)
    }

    pub fn todo() -> Error {
        Error::new(exception::runtime_error(), "not implemented yet")
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

#[macro_export]
macro_rules! raise_err(
    ($msg:expr, $err:ident) => {{
        Err(PolarsError::$err($msg.into())).map_err(RbPolarsErr::from)?;
        unreachable!()
    }}
);

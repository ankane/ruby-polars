use std::fmt::{Debug, Formatter};

use magnus::Error;
use polars::prelude::PolarsError;

use crate::exceptions::{AssertionError, ColumnNotFoundError, ComputeError, InvalidOperationError};
use crate::rb_modules;

pub enum RbPolarsErr {
    Polars(PolarsError),
    Other(String),
}

impl From<PolarsError> for RbPolarsErr {
    fn from(err: PolarsError) -> Self {
        RbPolarsErr::Polars(err)
    }
}

impl From<std::io::Error> for RbPolarsErr {
    fn from(value: std::io::Error) -> Self {
        RbPolarsErr::Other(format!("{value:?}"))
    }
}

impl From<RbPolarsErr> for Error {
    fn from(err: RbPolarsErr) -> Self {
        match err {
            RbPolarsErr::Polars(err) => match err {
                PolarsError::AssertionError(err) => AssertionError::new_err(err.to_string()),
                PolarsError::ColumnNotFound(name) => ColumnNotFoundError::new_err(name.to_string()),
                PolarsError::ComputeError(err) => ComputeError::new_err(err.to_string()),
                PolarsError::InvalidOperation(err) => {
                    InvalidOperationError::new_err(err.to_string())
                }
                _ => Error::new(rb_modules::error(), err.to_string()),
            },
            RbPolarsErr::Other(err) => Error::new(rb_modules::error(), err.to_string()),
        }
    }
}

impl Debug for RbPolarsErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use RbPolarsErr::*;
        match self {
            Polars(err) => write!(f, "{err:?}"),
            Other(err) => write!(f, "BindingsError: {err:?}"),
        }
    }
}

#[macro_export]
macro_rules! raise_err(
    ($msg:expr, $err:ident) => {{
        Err(PolarsError::$err($msg.into())).map_err(RbPolarsErr::from)?;
        unreachable!()
    }}
);

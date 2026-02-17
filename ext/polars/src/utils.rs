use std::panic::AssertUnwindSafe;

use magnus::Ruby;
use polars::frame::DataFrame;
use polars::series::IntoSeries;
use polars_error::PolarsResult;
use polars_error::signals::{KeyboardInterrupt, catch_keyboard_interrupt};

use crate::ruby::exceptions::RbKeyboardInterrupt;
use crate::ruby::gvl::GvlExt;
use crate::timeout::{cancel_polars_timeout, schedule_polars_timeout};
use crate::{RbDataFrame, RbErr, RbPolarsErr, RbResult, RbSeries};

#[macro_export]
macro_rules! apply_all_polars_dtypes {
    ($self:expr, $method:ident, $($args:expr),*) => {
        match $self.dtype() {
            DataType::Boolean => $self.bool().unwrap().$method($($args),*),
            DataType::String => $self.str().unwrap().$method($($args),*),
            DataType::UInt8 => $self.u8().unwrap().$method($($args),*),
            DataType::UInt16 => $self.u16().unwrap().$method($($args),*),
            DataType::UInt32 => $self.u32().unwrap().$method($($args),*),
            DataType::UInt64 => $self.u64().unwrap().$method($($args),*),
            DataType::UInt128 => $self.u128().unwrap().$method($($args),*),
            DataType::Int8 => $self.i8().unwrap().$method($($args),*),
            DataType::Int16 => $self.i16().unwrap().$method($($args),*),
            DataType::Int32 => $self.i32().unwrap().$method($($args),*),
            DataType::Int64 => $self.i64().unwrap().$method($($args),*),
            DataType::Int128 => $self.i128().unwrap().$method($($args),*),
            DataType::Float16 => todo!(),
            DataType::Float32 => $self.f32().unwrap().$method($($args),*),
            DataType::Float64 => $self.f64().unwrap().$method($($args),*),
            DataType::Date => $self.date().unwrap().physical().$method($($args),*),
            DataType::Datetime(_, _) => $self.datetime().unwrap().physical().$method($($args),*),
            // TODO implement
            // DataType::List(_) => $self.list().unwrap().$method($($args),*),
            DataType::Struct(_) => $self.struct_().unwrap().$method($($args),*),
            dt => panic!("dtype {:?} not supported", dt)
        }
    }
}

/// Boilerplate for `|e| RbPolarsErr::from(e).into()`
pub(crate) fn to_rb_err<E: Into<RbPolarsErr>>(e: E) -> RbErr {
    e.into().into()
}

pub trait EnterPolarsExt {
    fn enter_polars<T, E, F>(self, f: F) -> RbResult<T>
    where
        F: Send + FnOnce() -> Result<T, E>,
        T: Send,
        E: Send + Into<RbPolarsErr>;

    #[inline(always)]
    fn enter_polars_ok<T, F>(self, f: F) -> RbResult<T>
    where
        Self: Sized,
        F: Send + FnOnce() -> T,
        T: Send,
    {
        self.enter_polars(move || PolarsResult::Ok(f()))
    }

    #[inline(always)]
    fn enter_polars_df<F>(self, f: F) -> RbResult<RbDataFrame>
    where
        Self: Sized,
        F: Send + FnOnce() -> PolarsResult<DataFrame>,
    {
        self.enter_polars(f).map(RbDataFrame::new)
    }

    #[inline(always)]
    fn enter_polars_series<T, F>(self, f: F) -> RbResult<RbSeries>
    where
        Self: Sized,
        T: Send + IntoSeries,
        F: Send + FnOnce() -> PolarsResult<T>,
    {
        self.enter_polars(f).map(|s| RbSeries::new(s.into_series()))
    }
}

impl EnterPolarsExt for &Ruby {
    fn enter_polars<T, E, F>(self, f: F) -> RbResult<T>
    where
        F: Send + FnOnce() -> Result<T, E>,
        T: Send,
        E: Send + Into<RbPolarsErr>,
    {
        let timeout = schedule_polars_timeout();
        let ret = self.detach(|| catch_keyboard_interrupt(AssertUnwindSafe(f)));
        cancel_polars_timeout(timeout);
        match ret {
            Ok(Ok(ret)) => Ok(ret),
            Ok(Err(err)) => Err(RbErr::from(err.into())),
            Err(KeyboardInterrupt) => Err(RbKeyboardInterrupt::new_err("")),
        }
    }
}

use std::os::raw::c_void;
use std::panic::AssertUnwindSafe;

use magnus::Ruby;
use polars::frame::DataFrame;
use polars::series::IntoSeries;
use polars_error::PolarsResult;
use polars_error::signals::{KeyboardInterrupt, catch_keyboard_interrupt};
use rb_sys::{rb_thread_call_with_gvl, rb_thread_call_without_gvl};

use crate::exceptions::RbKeyboardInterrupt;
use crate::timeout::{cancel_polars_timeout, schedule_polars_timeout};
use crate::{RbDataFrame, RbErr, RbPolarsErr, RbResult, RbSeries};

#[macro_export]
macro_rules! apply_method_all_arrow_series2 {
    ($self:expr, $method:ident, $($args:expr),*) => {
        match $self.dtype() {
            DataType::Boolean => $self.bool().unwrap().$method($($args),*),
            DataType::String => $self.str().unwrap().$method($($args),*),
            DataType::UInt8 => $self.u8().unwrap().$method($($args),*),
            DataType::UInt16 => $self.u16().unwrap().$method($($args),*),
            DataType::UInt32 => $self.u32().unwrap().$method($($args),*),
            DataType::UInt64 => $self.u64().unwrap().$method($($args),*),
            DataType::Int8 => $self.i8().unwrap().$method($($args),*),
            DataType::Int16 => $self.i16().unwrap().$method($($args),*),
            DataType::Int32 => $self.i32().unwrap().$method($($args),*),
            DataType::Int64 => $self.i64().unwrap().$method($($args),*),
            DataType::Float32 => $self.f32().unwrap().$method($($args),*),
            DataType::Float64 => $self.f64().unwrap().$method($($args),*),
            DataType::Date => $self.date().unwrap().physical().$method($($args),*),
            DataType::Datetime(_, _) => $self.datetime().unwrap().physical().$method($($args),*),
            // DataType::List(_) => $self.list().unwrap().$method($($args),*),
            DataType::Struct(_) => $self.struct_().unwrap().$method($($args),*),
            dt => panic!("dtype {:?} not supported", dt)
        }
    }
}

/// Boilerplate for `|e| RbPolarsErr::from(e).into()`
#[allow(unused)]
pub(crate) fn to_rb_err<E: Into<RbPolarsErr>>(e: E) -> RbErr {
    e.into().into()
}

pub trait EnterPolarsExt {
    fn enter_polars<T, E, F>(self, f: F) -> RbResult<T>
    where
        F: FnOnce() -> Result<T, E>,
        E: Into<RbPolarsErr>;

    #[inline(always)]
    fn enter_polars_ok<T, F>(self, f: F) -> RbResult<T>
    where
        Self: Sized,
        F: FnOnce() -> T,
    {
        self.enter_polars(move || RbResult::Ok(f()))
    }

    #[inline(always)]
    fn enter_polars_df<F>(self, f: F) -> RbResult<RbDataFrame>
    where
        Self: Sized,
        F: FnOnce() -> PolarsResult<DataFrame>,
    {
        self.enter_polars(f).map(RbDataFrame::new)
    }

    #[inline(always)]
    fn enter_polars_series<T, F>(self, f: F) -> RbResult<RbSeries>
    where
        Self: Sized,
        T: IntoSeries,
        F: FnOnce() -> PolarsResult<T>,
    {
        self.enter_polars(f).map(|s| RbSeries::new(s.into_series()))
    }

    fn detach<T, F>(self, f: F) -> T
    where
        Self: Sized,
        F: FnOnce() -> T,
    {
        if std::env::var("POLARS_NO_GVL").is_ok() {
            let mut data = CallbackData {
                func: Some(f),
                result: None,
            };

            unsafe {
                rb_thread_call_without_gvl(
                    Some(call_without_gvl::<F, T>),
                    &mut data as *mut _ as *mut c_void,
                    None,
                    std::ptr::null_mut(),
                );
            }

            data.result.unwrap()
        } else {
            f()
        }
    }
}

impl EnterPolarsExt for &Ruby {
    fn enter_polars<T, E, F>(self, f: F) -> RbResult<T>
    where
        F: FnOnce() -> Result<T, E>,
        E: Into<RbPolarsErr>,
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

pub trait RubyAttach {
    fn attach<T, F>(f: F) -> T
    where
        F: FnOnce(&Ruby) -> T;
}

impl RubyAttach for Ruby {
    fn attach<T, F>(f: F) -> T
    where
        F: FnOnce(&Ruby) -> T,
    {
        if let Ok(rb) = Ruby::get() {
            f(&rb)
        } else {
            let mut data = CallbackData {
                func: Some(f),
                result: None,
            };

            unsafe {
                rb_thread_call_with_gvl(
                    Some(call_with_gvl::<F, T>),
                    &mut data as *mut _ as *mut c_void,
                );
            }

            data.result.unwrap()
        }
    }
}

struct CallbackData<F, T> {
    func: Option<F>,
    result: Option<T>,
}

extern "C" fn call_without_gvl<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> T,
{
    let data = unsafe { &mut *(data as *mut CallbackData<F, T>) };
    let func = data.func.take().unwrap();
    data.result = Some(func());
    std::ptr::null_mut()
}

extern "C" fn call_with_gvl<F, T>(data: *mut c_void) -> *mut c_void
where
    F: FnOnce(&Ruby) -> T,
{
    let rb = Ruby::get().unwrap();
    let data = unsafe { &mut *(data as *mut CallbackData<F, T>) };
    let func = data.func.take().unwrap();
    data.result = Some(func(&rb));
    std::ptr::null_mut()
}

use magnus::Ruby;

use crate::{RbErr, RbPolarsErr, RbResult};

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
        // TODO use enter_polars
        RbResult::Ok(f())
    }
}

impl EnterPolarsExt for &Ruby {
    fn enter_polars<T, E, F>(self, f: F) -> RbResult<T>
    where
        F: FnOnce() -> Result<T, E>,
        E: Into<RbPolarsErr>,
    {
        let ret = f();
        match ret {
            Ok(ret) => Ok(ret),
            Err(err) => Err(RbErr::from(err.into())),
        }
    }
}

use crate::{RbPolarsErr, RbResult, RbSeries};

impl RbSeries {
    pub fn add(&self, other: &RbSeries) -> RbResult<Self> {
        Ok((&*self.series.borrow() + &*other.series.borrow())
            .map(Into::into)
            .map_err(RbPolarsErr::from)?)
    }

    pub fn sub(&self, other: &RbSeries) -> RbResult<Self> {
        Ok((&*self.series.borrow() - &*other.series.borrow())
            .map(Into::into)
            .map_err(RbPolarsErr::from)?)
    }

    pub fn mul(&self, other: &RbSeries) -> RbResult<Self> {
        Ok((&*self.series.borrow() * &*other.series.borrow())
            .map(Into::into)
            .map_err(RbPolarsErr::from)?)
    }

    pub fn div(&self, other: &RbSeries) -> RbResult<Self> {
        Ok((&*self.series.borrow() / &*other.series.borrow())
            .map(Into::into)
            .map_err(RbPolarsErr::from)?)
    }

    pub fn rem(&self, other: &RbSeries) -> RbResult<Self> {
        Ok((&*self.series.borrow() % &*other.series.borrow())
            .map(Into::into)
            .map_err(RbPolarsErr::from)?)
    }
}

macro_rules! impl_arithmetic {
    ($name:ident, $type:ty, $operand:tt) => {
        impl RbSeries {
            pub fn $name(&self, other: $type) -> RbResult<Self> {
                Ok(RbSeries::new(&*self.series.borrow() $operand other))
            }
        }
    };
}

impl_arithmetic!(add_u8, u8, +);
impl_arithmetic!(add_u16, u16, +);
impl_arithmetic!(add_u32, u32, +);
impl_arithmetic!(add_u64, u64, +);
impl_arithmetic!(add_i8, i8, +);
impl_arithmetic!(add_i16, i16, +);
impl_arithmetic!(add_i32, i32, +);
impl_arithmetic!(add_i64, i64, +);
impl_arithmetic!(add_datetime, i64, +);
impl_arithmetic!(add_duration, i64, +);
impl_arithmetic!(add_f32, f32, +);
impl_arithmetic!(add_f64, f64, +);
impl_arithmetic!(sub_u8, u8, -);
impl_arithmetic!(sub_u16, u16, -);
impl_arithmetic!(sub_u32, u32, -);
impl_arithmetic!(sub_u64, u64, -);
impl_arithmetic!(sub_i8, i8, -);
impl_arithmetic!(sub_i16, i16, -);
impl_arithmetic!(sub_i32, i32, -);
impl_arithmetic!(sub_i64, i64, -);
impl_arithmetic!(sub_datetime, i64, -);
impl_arithmetic!(sub_duration, i64, -);
impl_arithmetic!(sub_f32, f32, -);
impl_arithmetic!(sub_f64, f64, -);
impl_arithmetic!(div_u8, u8, /);
impl_arithmetic!(div_u16, u16, /);
impl_arithmetic!(div_u32, u32, /);
impl_arithmetic!(div_u64, u64, /);
impl_arithmetic!(div_i8, i8, /);
impl_arithmetic!(div_i16, i16, /);
impl_arithmetic!(div_i32, i32, /);
impl_arithmetic!(div_i64, i64, /);
impl_arithmetic!(div_f32, f32, /);
impl_arithmetic!(div_f64, f64, /);
impl_arithmetic!(mul_u8, u8, *);
impl_arithmetic!(mul_u16, u16, *);
impl_arithmetic!(mul_u32, u32, *);
impl_arithmetic!(mul_u64, u64, *);
impl_arithmetic!(mul_i8, i8, *);
impl_arithmetic!(mul_i16, i16, *);
impl_arithmetic!(mul_i32, i32, *);
impl_arithmetic!(mul_i64, i64, *);
impl_arithmetic!(mul_f32, f32, *);
impl_arithmetic!(mul_f64, f64, *);
impl_arithmetic!(rem_u8, u8, %);
impl_arithmetic!(rem_u16, u16, %);
impl_arithmetic!(rem_u32, u32, %);
impl_arithmetic!(rem_u64, u64, %);
impl_arithmetic!(rem_i8, i8, %);
impl_arithmetic!(rem_i16, i16, %);
impl_arithmetic!(rem_i32, i32, %);
impl_arithmetic!(rem_i64, i64, %);
impl_arithmetic!(rem_f32, f32, %);
impl_arithmetic!(rem_f64, f64, %);

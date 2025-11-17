use magnus::Ruby;

use crate::error::RbPolarsErr;
use crate::prelude::*;
use crate::utils::EnterPolarsExt;
use crate::{RbResult, RbSeries};

impl RbSeries {
    pub fn eq(rb: &Ruby, self_: &Self, rhs: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().equal(&*rhs.series.read()))
    }

    pub fn neq(rb: &Ruby, self_: &Self, rhs: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().not_equal(&*rhs.series.read()))
    }

    pub fn gt(rb: &Ruby, self_: &Self, rhs: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().gt(&*rhs.series.read()))
    }

    pub fn gt_eq(rb: &Ruby, self_: &Self, rhs: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().gt_eq(&*rhs.series.read()))
    }

    pub fn lt(rb: &Ruby, self_: &Self, rhs: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().lt(&*rhs.series.read()))
    }

    pub fn lt_eq(rb: &Ruby, self_: &Self, rhs: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().lt_eq(&*rhs.series.read()))
    }
}

macro_rules! impl_op {
    ($op:ident, $name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(rb: &Ruby, self_: &Self, rhs: $type) -> RbResult<Self> {
                rb.enter_polars_series(|| self_.series.read().$op(rhs))
            }
        }
    };
}

impl_op!(equal, eq_u8, u8);
impl_op!(equal, eq_u16, u16);
impl_op!(equal, eq_u32, u32);
impl_op!(equal, eq_u64, u64);
impl_op!(equal, eq_i8, i8);
impl_op!(equal, eq_i16, i16);
impl_op!(equal, eq_i32, i32);
impl_op!(equal, eq_i64, i64);
impl_op!(equal, eq_i128, i128);
impl_op!(equal, eq_f32, f32);
impl_op!(equal, eq_f64, f64);

impl_op!(not_equal, neq_u8, u8);
impl_op!(not_equal, neq_u16, u16);
impl_op!(not_equal, neq_u32, u32);
impl_op!(not_equal, neq_u64, u64);
impl_op!(not_equal, neq_i8, i8);
impl_op!(not_equal, neq_i16, i16);
impl_op!(not_equal, neq_i32, i32);
impl_op!(not_equal, neq_i64, i64);
impl_op!(not_equal, neq_i128, i128);
impl_op!(not_equal, neq_f32, f32);
impl_op!(not_equal, neq_f64, f64);

impl_op!(gt, gt_u8, u8);
impl_op!(gt, gt_u16, u16);
impl_op!(gt, gt_u32, u32);
impl_op!(gt, gt_u64, u64);
impl_op!(gt, gt_i8, i8);
impl_op!(gt, gt_i16, i16);
impl_op!(gt, gt_i32, i32);
impl_op!(gt, gt_i64, i64);
impl_op!(gt, gt_i128, i128);
impl_op!(gt, gt_f32, f32);
impl_op!(gt, gt_f64, f64);

impl_op!(gt_eq, gt_eq_u8, u8);
impl_op!(gt_eq, gt_eq_u16, u16);
impl_op!(gt_eq, gt_eq_u32, u32);
impl_op!(gt_eq, gt_eq_u64, u64);
impl_op!(gt_eq, gt_eq_i8, i8);
impl_op!(gt_eq, gt_eq_i16, i16);
impl_op!(gt_eq, gt_eq_i32, i32);
impl_op!(gt_eq, gt_eq_i64, i64);
impl_op!(gt_eq, gt_eq_i128, i128);
impl_op!(gt_eq, gt_eq_f32, f32);
impl_op!(gt_eq, gt_eq_f64, f64);

impl_op!(lt, lt_u8, u8);
impl_op!(lt, lt_u16, u16);
impl_op!(lt, lt_u32, u32);
impl_op!(lt, lt_u64, u64);
impl_op!(lt, lt_i8, i8);
impl_op!(lt, lt_i16, i16);
impl_op!(lt, lt_i32, i32);
impl_op!(lt, lt_i64, i64);
impl_op!(lt, lt_i128, i128);
impl_op!(lt, lt_f32, f32);
impl_op!(lt, lt_f64, f64);

impl_op!(lt_eq, lt_eq_u8, u8);
impl_op!(lt_eq, lt_eq_u16, u16);
impl_op!(lt_eq, lt_eq_u32, u32);
impl_op!(lt_eq, lt_eq_u64, u64);
impl_op!(lt_eq, lt_eq_i8, i8);
impl_op!(lt_eq, lt_eq_i16, i16);
impl_op!(lt_eq, lt_eq_i32, i32);
impl_op!(lt_eq, lt_eq_i64, i64);
impl_op!(lt_eq, lt_eq_i128, i128);
impl_op!(lt_eq, lt_eq_f32, f32);
impl_op!(lt_eq, lt_eq_f64, f64);

impl RbSeries {
    pub fn eq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .read()
            .equal(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn neq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .read()
            .not_equal(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn gt_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .read()
            .gt(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn gt_eq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .read()
            .gt_eq(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn lt_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .read()
            .lt(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn lt_eq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .read()
            .lt_eq(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }
}

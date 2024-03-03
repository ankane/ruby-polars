use crate::error::RbPolarsErr;
use crate::prelude::*;
use crate::{RbResult, RbSeries, RbValueError};
use magnus::{IntoValue, Value};

impl RbSeries {
    pub fn any(&self, ignore_nulls: bool) -> RbResult<Option<bool>> {
        let binding = self.series.borrow();
        let s = binding.bool().map_err(RbPolarsErr::from)?;
        Ok(if ignore_nulls {
            Some(s.any())
        } else {
            s.any_kleene()
        })
    }

    pub fn all(&self, ignore_nulls: bool) -> RbResult<Option<bool>> {
        let binding = self.series.borrow();
        let s = binding.bool().map_err(RbPolarsErr::from)?;
        Ok(if ignore_nulls {
            Some(s.all())
        } else {
            s.all_kleene()
        })
    }

    pub fn arg_max(&self) -> Option<usize> {
        self.series.borrow().arg_max()
    }

    pub fn arg_min(&self) -> Option<usize> {
        self.series.borrow().arg_min()
    }

    pub fn max(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .max_as_series()
                .map_err(RbPolarsErr::from)?
                .get(0)
                .map_err(RbPolarsErr::from)?,
        )
        .into_value())
    }

    pub fn mean(&self) -> Option<f64> {
        match self.series.borrow().dtype() {
            DataType::Boolean => {
                let s = self.series.borrow().cast(&DataType::UInt8).unwrap();
                s.mean()
            }
            _ => self.series.borrow().mean(),
        }
    }

    pub fn median(&self) -> Option<f64> {
        match self.series.borrow().dtype() {
            DataType::Boolean => {
                let s = self.series.borrow().cast(&DataType::UInt8).unwrap();
                s.median()
            }
            _ => self.series.borrow().median(),
        }
    }

    pub fn min(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .min_as_series()
                .map_err(RbPolarsErr::from)?
                .get(0)
                .map_err(RbPolarsErr::from)?,
        )
        .into_value())
    }

    pub fn quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .quantile_as_series(quantile, interpolation.0)
                .map_err(|_| RbValueError::new_err("invalid quantile".into()))?
                .get(0)
                .unwrap_or(AnyValue::Null),
        )
        .into_value())
    }

    pub fn sum(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .sum_as_series()
                .map_err(RbPolarsErr::from)?
                .get(0)
                .map_err(RbPolarsErr::from)?,
        )
        .into_value())
    }
}

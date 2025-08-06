use crate::error::RbPolarsErr;
use crate::prelude::*;
use crate::{RbResult, RbSeries};
use magnus::{IntoValue, Value};

fn scalar_to_rb(scalar: RbResult<Scalar>) -> RbResult<Value> {
    Ok(Wrap(scalar?.as_any_value()).into_value())
}

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
                .max_reduce()
                .map_err(RbPolarsErr::from)?
                .as_any_value(),
        )
        .into_value())
    }

    pub fn mean(&self) -> RbResult<Value> {
        match self.series.borrow().dtype() {
            DataType::Boolean => Ok(Wrap(
                self.series
                    .borrow()
                    .cast(&DataType::UInt8)
                    .unwrap()
                    .mean_reduce()
                    .as_any_value(),
            )
            .into_value()),
            // For non-numeric output types we require mean_reduce.
            dt if dt.is_temporal() => {
                Ok(Wrap(self.series.borrow().mean_reduce().as_any_value()).into_value())
            }
            _ => Ok(self.series.borrow().mean().into_value()),
        }
    }

    pub fn median(&self) -> RbResult<Value> {
        match self.series.borrow().dtype() {
            DataType::Boolean => Ok(Wrap(
                self.series
                    .borrow()
                    .cast(&DataType::UInt8)
                    .unwrap()
                    .median_reduce()
                    .map_err(RbPolarsErr::from)?
                    .as_any_value(),
            )
            .into_value()),
            // For non-numeric output types we require median_reduce.
            dt if dt.is_temporal() => Ok(Wrap(
                self.series
                    .borrow()
                    .median_reduce()
                    .map_err(RbPolarsErr::from)?
                    .as_any_value(),
            )
            .into_value()),
            _ => Ok(self.series.borrow().median().into_value()),
        }
    }

    pub fn min(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .min_reduce()
                .map_err(RbPolarsErr::from)?
                .as_any_value(),
        )
        .into_value())
    }

    pub fn quantile(&self, quantile: f64, interpolation: Wrap<QuantileMethod>) -> RbResult<Value> {
        let bind = self
            .series
            .borrow()
            .quantile_reduce(quantile, interpolation.0);
        let sc = bind.map_err(RbPolarsErr::from)?;

        Ok(Wrap(sc.as_any_value()).into_value())
    }

    pub fn sum(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .sum_reduce()
                .map_err(RbPolarsErr::from)?
                .as_any_value(),
        )
        .into_value())
    }

    pub fn first(&self) -> RbResult<Value> {
        scalar_to_rb(Ok(self.series.borrow().first()))
    }

    pub fn last(&self) -> RbResult<Value> {
        scalar_to_rb(Ok(self.series.borrow().last()))
    }

    pub fn approx_n_unique(&self) -> RbResult<IdxSize> {
        Ok(self
            .series
            .borrow()
            .approx_n_unique()
            .map_err(RbPolarsErr::from)?)
    }

    pub fn bitwise_and(&self) -> RbResult<Value> {
        scalar_to_rb(Ok(self
            .series
            .borrow()
            .and_reduce()
            .map_err(RbPolarsErr::from)?))
    }

    pub fn bitwise_or(&self) -> RbResult<Value> {
        scalar_to_rb(Ok(self
            .series
            .borrow()
            .or_reduce()
            .map_err(RbPolarsErr::from)?))
    }

    pub fn bitwise_xor(&self) -> RbResult<Value> {
        scalar_to_rb(Ok(self
            .series
            .borrow()
            .xor_reduce()
            .map_err(RbPolarsErr::from)?))
    }
}

use crate::error::RbPolarsErr;
use crate::prelude::*;
use crate::utils::to_rb_err;
use crate::{RbResult, RbSeries};
use magnus::{IntoValue, Ruby, Value};

fn scalar_to_rb(scalar: RbResult<Scalar>) -> RbResult<Value> {
    let ruby = Ruby::get().unwrap();
    Ok(Wrap(scalar?.as_any_value()).into_value_with(&ruby))
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

    pub fn max(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        Ok(Wrap(
            self_
                .series
                .borrow()
                .max_reduce()
                .map_err(RbPolarsErr::from)?
                .as_any_value(),
        )
        .into_value_with(ruby))
    }

    pub fn mean(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        match self_.series.borrow().dtype() {
            DataType::Boolean => Ok(Wrap(
                self_
                    .series
                    .borrow()
                    .cast(&DataType::UInt8)
                    .unwrap()
                    .mean_reduce()
                    .map_err(to_rb_err)?
                    .as_any_value(),
            )
            .into_value_with(ruby)),
            // For non-numeric output types we require mean_reduce.
            dt if dt.is_temporal() => Ok(Wrap(
                self_
                    .series
                    .borrow()
                    .mean_reduce()
                    .map_err(to_rb_err)?
                    .as_any_value(),
            )
            .into_value_with(ruby)),
            _ => Ok(self_.series.borrow().mean().into_value_with(ruby)),
        }
    }

    pub fn median(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        match self_.series.borrow().dtype() {
            DataType::Boolean => Ok(Wrap(
                self_
                    .series
                    .borrow()
                    .cast(&DataType::UInt8)
                    .unwrap()
                    .median_reduce()
                    .map_err(RbPolarsErr::from)?
                    .as_any_value(),
            )
            .into_value_with(ruby)),
            // For non-numeric output types we require median_reduce.
            dt if dt.is_temporal() => Ok(Wrap(
                self_
                    .series
                    .borrow()
                    .median_reduce()
                    .map_err(RbPolarsErr::from)?
                    .as_any_value(),
            )
            .into_value_with(ruby)),
            _ => Ok(self_.series.borrow().median().into_value_with(ruby)),
        }
    }

    pub fn min(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        Ok(Wrap(
            self_
                .series
                .borrow()
                .min_reduce()
                .map_err(RbPolarsErr::from)?
                .as_any_value(),
        )
        .into_value_with(ruby))
    }

    pub fn quantile(
        ruby: &Ruby,
        self_: &Self,
        quantile: f64,
        interpolation: Wrap<QuantileMethod>,
    ) -> RbResult<Value> {
        let bind = self_
            .series
            .borrow()
            .quantile_reduce(quantile, interpolation.0);
        let sc = bind.map_err(RbPolarsErr::from)?;

        Ok(Wrap(sc.as_any_value()).into_value_with(ruby))
    }

    pub fn sum(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        Ok(Wrap(
            self_
                .series
                .borrow()
                .sum_reduce()
                .map_err(RbPolarsErr::from)?
                .as_any_value(),
        )
        .into_value_with(ruby))
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

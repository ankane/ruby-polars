use DataType::*;
use magnus::{IntoValue, Ruby, Value};
use polars::prelude::*;

use crate::prelude::*;
use crate::utils::EnterPolarsExt;
use crate::{RbResult, RbSeries};

fn scalar_to_rb(scalar: RbResult<Scalar>, rb: &Ruby) -> RbResult<Value> {
    Ok(Wrap(scalar?.as_any_value()).into_value_with(rb))
}

impl RbSeries {
    pub fn any(rb: &Ruby, self_: &Self, ignore_nulls: bool) -> RbResult<Option<bool>> {
        rb.enter_polars(|| {
            let s = self_.series.read();
            let s = s.bool()?;
            PolarsResult::Ok(if ignore_nulls {
                Some(s.any())
            } else {
                s.any_kleene()
            })
        })
    }

    pub fn all(rb: &Ruby, self_: &Self, ignore_nulls: bool) -> RbResult<Option<bool>> {
        rb.enter_polars(|| {
            let s = self_.series.read();
            let s = s.bool()?;
            PolarsResult::Ok(if ignore_nulls {
                Some(s.all())
            } else {
                s.all_kleene()
            })
        })
    }

    pub fn arg_max(rb: &Ruby, self_: &Self) -> RbResult<Option<usize>> {
        rb.enter_polars_ok(|| self_.series.read().arg_max())
    }

    pub fn arg_min(rb: &Ruby, self_: &Self) -> RbResult<Option<usize>> {
        rb.enter_polars_ok(|| self_.series.read().arg_min())
    }

    pub fn min(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars(|| self_.series.read().min_reduce()), rb)
    }

    pub fn max(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars(|| self_.series.read().max_reduce()), rb)
    }

    pub fn mean(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        let s = self_.series.read();
        match s.dtype() {
            Boolean => scalar_to_rb(
                rb.enter_polars(|| s.cast(&DataType::UInt8).unwrap().mean_reduce()),
                rb,
            ),
            // For non-numeric output types we require mean_reduce.
            dt if dt.is_temporal() => scalar_to_rb(rb.enter_polars(|| s.mean_reduce()), rb),
            _ => Ok(s.mean().into_value_with(rb)),
        }
    }

    pub fn median(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        let s = self_.series.read();
        match s.dtype() {
            Boolean => scalar_to_rb(
                rb.enter_polars(|| s.cast(&DataType::UInt8).unwrap().median_reduce()),
                rb,
            ),
            // For non-numeric output types we require median_reduce.
            dt if dt.is_temporal() => scalar_to_rb(rb.enter_polars(|| s.median_reduce()), rb),
            _ => Ok(s.median().into_value_with(rb)),
        }
    }

    pub fn quantile(
        rb: &Ruby,
        self_: &Self,
        quantile: f64,
        interpolation: Wrap<QuantileMethod>,
    ) -> RbResult<Value> {
        scalar_to_rb(
            rb.enter_polars(|| {
                self_
                    .series
                    .read()
                    .quantile_reduce(quantile, interpolation.0)
            }),
            rb,
        )
    }

    pub fn sum(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars(|| self_.series.read().sum_reduce()), rb)
    }

    pub fn first(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars_ok(|| self_.series.read().first()), rb)
    }

    pub fn last(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars_ok(|| self_.series.read().last()), rb)
    }

    pub fn approx_n_unique(rb: &Ruby, self_: &Self) -> RbResult<IdxSize> {
        rb.enter_polars(|| self_.series.read().approx_n_unique())
    }

    pub fn bitwise_and(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars(|| self_.series.read().and_reduce()), rb)
    }

    pub fn bitwise_or(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars(|| self_.series.read().or_reduce()), rb)
    }

    pub fn bitwise_xor(rb: &Ruby, self_: &Self) -> RbResult<Value> {
        scalar_to_rb(rb.enter_polars(|| self_.series.read().xor_reduce()), rb)
    }
}

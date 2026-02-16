use magnus::Error;
use polars::error::PolarsError;

pub(crate) fn to_pl_err(e: Error) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

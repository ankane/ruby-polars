use magnus::Value;
use polars::error::PolarsResult;
use polars::series::Series;

pub fn binary_lambda(lambda: Value, a: Series, b: Series) -> PolarsResult<Series> {
    todo!();
}

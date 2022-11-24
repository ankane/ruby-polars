use magnus::Value;
use polars::error::PolarsResult;
use polars::series::Series;

pub fn binary_lambda(_lambda: Value, _a: Series, _b: Series) -> PolarsResult<Series> {
    todo!();
}

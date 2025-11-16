use polars_testing::asserts::{SeriesEqualOptions, assert_series_equal};

use crate::error::RbPolarsErr;
use crate::{RbResult, RbSeries};

pub fn assert_series_equal_rb(
    left: &RbSeries,
    right: &RbSeries,
    check_dtypes: bool,
    check_names: bool,
    check_order: bool,
    check_exact: bool,
    rel_tol: f64,
    abs_tol: f64,
    categorical_as_str: bool,
) -> RbResult<()> {
    let left_series = &left.series.borrow();
    let right_series = &right.series.borrow();

    let options = SeriesEqualOptions {
        check_dtypes,
        check_names,
        check_order,
        check_exact,
        rel_tol,
        abs_tol,
        categorical_as_str,
    };

    assert_series_equal(left_series, right_series, options).map_err(|e| RbPolarsErr::from(e).into())
}

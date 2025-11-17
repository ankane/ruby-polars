use polars_testing::asserts::{DataFrameEqualOptions, assert_dataframe_equal};

use crate::error::RbPolarsErr;
use crate::{RbDataFrame, RbResult};

pub fn assert_dataframe_equal_rb(
    left: &RbDataFrame,
    right: &RbDataFrame,
    check_row_order: bool,
    check_column_order: bool,
    check_dtypes: bool,
    check_exact: bool,
    rel_tol: f64,
    abs_tol: f64,
    categorical_as_str: bool,
) -> RbResult<()> {
    let left_df = &left.df.read();
    let right_df = &right.df.read();

    let options = DataFrameEqualOptions {
        check_row_order,
        check_column_order,
        check_dtypes,
        check_exact,
        rel_tol,
        abs_tol,
        categorical_as_str,
    };

    assert_dataframe_equal(left_df, right_df, options).map_err(|e| RbPolarsErr::from(e).into())
}

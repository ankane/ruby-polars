use polars::lazy::dsl;

use crate::RbExpr;

pub fn business_day_count(
    start: &RbExpr,
    end: &RbExpr,
    week_mask: [bool; 7],
    holidays: Vec<i32>,
) -> RbExpr {
    let start = start.inner.clone();
    let end = end.inner.clone();
    dsl::business_day_count(start, end, week_mask, holidays).into()
}

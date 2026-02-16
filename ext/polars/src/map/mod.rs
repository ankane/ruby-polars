pub mod lazy;
pub mod series;

use magnus::{Ruby, Value, prelude::*};
use polars::prelude::*;

use crate::{RbResult, RbSeries};

pub trait RbPolarsNumericType: PolarsNumericType {}

impl RbPolarsNumericType for UInt8Type {}
impl RbPolarsNumericType for UInt16Type {}
impl RbPolarsNumericType for UInt32Type {}
impl RbPolarsNumericType for UInt64Type {}
impl RbPolarsNumericType for UInt128Type {}
impl RbPolarsNumericType for Int8Type {}
impl RbPolarsNumericType for Int16Type {}
impl RbPolarsNumericType for Int32Type {}
impl RbPolarsNumericType for Int64Type {}
impl RbPolarsNumericType for Int128Type {}
impl RbPolarsNumericType for Float16Type {}
impl RbPolarsNumericType for Float32Type {}
impl RbPolarsNumericType for Float64Type {}

pub mod dataframe;
pub mod lazy;
pub mod series;

use magnus::{Ruby, Value, prelude::*};
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::*;
use polars_core::utils::CustomIterTools;

use crate::{RbPolarsErr, RbResult, RbSeries};

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

fn iterator_to_primitive<T>(
    it: impl Iterator<Item = Option<T::Native>>,
    init_null_count: usize,
    first_value: Option<T::Native>,
    name: PlSmallStr,
    capacity: usize,
) -> ChunkedArray<T>
where
    T: RbPolarsNumericType,
{
    // safety: we know the iterators len
    let mut ca: ChunkedArray<T> = unsafe {
        if init_null_count > 0 {
            (0..init_null_count)
                .map(|_| None)
                .chain(std::iter::once(first_value))
                .chain(it)
                .trust_my_length(capacity)
                .collect_trusted()
        } else if first_value.is_some() {
            std::iter::once(first_value)
                .chain(it)
                .trust_my_length(capacity)
                .collect_trusted()
        } else {
            it.collect()
        }
    };
    debug_assert_eq!(ca.len(), capacity);
    ca.rename(name);
    ca
}

fn iterator_to_bool(
    it: impl Iterator<Item = Option<bool>>,
    init_null_count: usize,
    first_value: Option<bool>,
    name: PlSmallStr,
    capacity: usize,
) -> ChunkedArray<BooleanType> {
    // safety: we know the iterators len
    let mut ca: BooleanChunked = unsafe {
        if init_null_count > 0 {
            (0..init_null_count)
                .map(|_| None)
                .chain(std::iter::once(first_value))
                .chain(it)
                .trust_my_length(capacity)
                .collect_trusted()
        } else if first_value.is_some() {
            std::iter::once(first_value)
                .chain(it)
                .trust_my_length(capacity)
                .collect_trusted()
        } else {
            it.collect()
        }
    };
    debug_assert_eq!(ca.len(), capacity);
    ca.rename(name);
    ca
}

fn iterator_to_utf8(
    it: impl Iterator<Item = Option<String>>,
    init_null_count: usize,
    first_value: Option<&str>,
    name: PlSmallStr,
    capacity: usize,
) -> StringChunked {
    let first_value = first_value.map(|v| v.to_string());

    // safety: we know the iterators len
    let mut ca: StringChunked = unsafe {
        if init_null_count > 0 {
            (0..init_null_count)
                .map(|_| None)
                .chain(std::iter::once(first_value))
                .chain(it)
                .trust_my_length(capacity)
                .collect_trusted()
        } else if first_value.is_some() {
            std::iter::once(first_value)
                .chain(it)
                .trust_my_length(capacity)
                .collect_trusted()
        } else {
            it.collect()
        }
    };
    debug_assert_eq!(ca.len(), capacity);
    ca.rename(name);
    ca
}

fn iterator_to_list(
    dt: &DataType,
    it: impl Iterator<Item = Option<Series>>,
    init_null_count: usize,
    first_value: Option<&Series>,
    name: PlSmallStr,
    capacity: usize,
) -> RbResult<ListChunked> {
    let mut builder = get_list_builder(dt, capacity * 5, capacity, name);
    for _ in 0..init_null_count {
        builder.append_null()
    }
    builder
        .append_opt_series(first_value)
        .map_err(RbPolarsErr::from)?;
    for opt_val in it {
        match opt_val {
            None => builder.append_null(),
            Some(s) => {
                if s.is_empty() && s.dtype() != dt {
                    builder
                        .append_series(&Series::full_null(PlSmallStr::EMPTY, 0, dt))
                        .unwrap()
                } else {
                    builder.append_series(&s).map_err(RbPolarsErr::from)?
                }
            }
        }
    }
    Ok(builder.finish())
}

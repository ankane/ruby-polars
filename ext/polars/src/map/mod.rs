pub mod dataframe;
pub mod lazy;
pub mod series;

use magnus::{RHash, Ruby, Value, prelude::*};
use polars::chunked_array::builder::get_list_builder;
use polars::prelude::*;
use polars_core::POOL;
use polars_core::utils::CustomIterTools;
use rayon::prelude::*;

use crate::utils::EnterPolarsExt;
use crate::{ObjectValue, RbPolarsErr, RbResult, RbSeries, RbValueError, Wrap};

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

pub(super) fn check_nested_object(dt: &DataType) -> RbResult<()> {
    if dt.contains_objects() {
        Err(RbValueError::new_err(
            "nested objects are not allowed\n\nSet `return_dtype: Polars::Object` to use Ruby's native nesting.",
        ))
    } else {
        Ok(())
    }
}

fn iterator_to_struct(
    it: impl Iterator<Item = Option<Value>>,
    init_null_count: usize,
    first_value: AnyValue,
    name: PlSmallStr,
    capacity: usize,
) -> RbResult<RbSeries> {
    let rb = Ruby::get().unwrap();

    let (vals, flds) = match &first_value {
        av @ AnyValue::Struct(_, _, flds) => (av._iter_struct_av().collect::<Vec<_>>(), &**flds),
        AnyValue::StructOwned(payload) => (payload.0.clone(), &*payload.1),
        _ => {
            return Err(crate::exceptions::ComputeError::new_err(format!(
                "expected struct got {first_value:?}",
            )));
        }
    };

    let struct_width = vals.len();

    // every item in the struct is kept as its own buffer of anyvalues
    // so as struct with 2 items: {a, b}
    // will have
    // [
    //      [ a values ]
    //      [ b values ]
    // ]
    let mut items = Vec::with_capacity(vals.len());
    for item in vals {
        let mut buf = Vec::with_capacity(capacity);
        for _ in 0..init_null_count {
            buf.push(AnyValue::Null);
        }
        buf.push(item.clone());
        items.push(buf);
    }

    for dict in it {
        match dict {
            None => {
                for field_items in &mut items {
                    field_items.push(AnyValue::Null);
                }
            }
            Some(dict) => {
                let dict = RHash::try_convert(dict)?;
                if dict.len() != struct_width {
                    return Err(crate::exceptions::ComputeError::new_err(format!(
                        "Cannot create struct type.\n> The struct dtype expects {} fields, but it got a dict with {} fields.",
                        struct_width,
                        dict.len()
                    )));
                }
                // we ignore the keys of the rest of the dicts
                // the first item determines the output name
                todo!()
                // for ((_, val), field_items) in dict.iter().zip(&mut items) {
                //     let item = Wrap::<AnyValue>::try_convert(val)?;
                //     field_items.push(item.0)
                // }
            }
        }
    }

    let fields = rb.enter_polars_ok(|| {
        POOL.install(|| {
            items
                .par_iter()
                .zip(flds)
                .map(|(av, fld)| Series::new(fld.name().clone(), av))
                .collect::<Vec<_>>()
        })
    })?;

    Ok(
        StructChunked::from_series(name, fields[0].len(), fields.iter())
            .unwrap()
            .into_series()
            .into(),
    )
}

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

fn iterator_to_object(
    it: impl Iterator<Item = Option<ObjectValue>>,
    init_null_count: usize,
    first_value: Option<ObjectValue>,
    name: PlSmallStr,
    capacity: usize,
) -> ObjectChunked<ObjectValue> {
    // safety: we know the iterators len
    let mut ca: ObjectChunked<ObjectValue> = unsafe {
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

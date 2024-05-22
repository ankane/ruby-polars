mod aggregation;
mod arithmetic;
mod comparison;
mod construction;
mod export;
mod scatter;

use magnus::{exception, prelude::*, value::qnil, Error, IntoValue, RArray, Value};
use polars::prelude::*;
use polars::series::IsSorted;
use std::cell::RefCell;

use crate::apply_method_all_arrow_series2;
use crate::conversion::*;
use crate::map::series::{call_lambda_and_extract, ApplyLambda};
use crate::{RbDataFrame, RbPolarsErr, RbResult};

#[magnus::wrap(class = "Polars::RbSeries")]
pub struct RbSeries {
    pub series: RefCell<Series>,
}

impl From<Series> for RbSeries {
    fn from(series: Series) -> Self {
        RbSeries::new(series)
    }
}

impl RbSeries {
    pub fn new(series: Series) -> Self {
        RbSeries {
            series: RefCell::new(series),
        }
    }
}

pub fn to_series_collection(rs: RArray) -> RbResult<Vec<Series>> {
    let mut series = Vec::new();
    for item in rs.each() {
        series.push(<&RbSeries>::try_convert(item?)?.series.borrow().clone());
    }
    Ok(series)
}

pub fn to_rbseries_collection(s: Vec<Series>) -> RArray {
    RArray::from_iter(s.into_iter().map(RbSeries::new))
}

impl RbSeries {
    pub fn struct_unnest(&self) -> RbResult<RbDataFrame> {
        let binding = self.series.borrow();
        let ca = binding.struct_().map_err(RbPolarsErr::from)?;
        let df: DataFrame = ca.clone().into();
        Ok(df.into())
    }

    // pub fn struct_fields(&self) -> RbResult<Vec<&str>> {
    //     let ca = self.series.borrow().struct_().map_err(RbPolarsErr::from)?;
    //     Ok(ca.fields().iter().map(|s| s.name()).collect())
    // }

    pub fn is_sorted_ascending_flag(&self) -> bool {
        matches!(self.series.borrow().is_sorted_flag(), IsSorted::Ascending)
    }

    pub fn is_sorted_descending_flag(&self) -> bool {
        matches!(self.series.borrow().is_sorted_flag(), IsSorted::Descending)
    }

    pub fn can_fast_explode_flag(&self) -> bool {
        match self.series.borrow().list() {
            Err(_) => false,
            Ok(list) => list._can_fast_explode(),
        }
    }

    pub fn cat_uses_lexical_ordering(&self) -> RbResult<bool> {
        let binding = self.series.borrow();
        let ca = binding.categorical().map_err(RbPolarsErr::from)?;
        Ok(ca.uses_lexical_ordering())
    }

    pub fn cat_is_local(&self) -> RbResult<bool> {
        let binding = self.series.borrow();
        let ca = binding.categorical().map_err(RbPolarsErr::from)?;
        Ok(ca.get_rev_map().is_local())
    }

    pub fn cat_to_local(&self) -> RbResult<Self> {
        let binding = self.series.borrow();
        let ca = binding.categorical().map_err(RbPolarsErr::from)?;
        Ok(ca.to_local().into_series().into())
    }

    pub fn estimated_size(&self) -> usize {
        self.series.borrow().estimated_size()
    }

    pub fn get_fmt(&self, index: usize, str_lengths: usize) -> String {
        let val = format!("{}", self.series.borrow().get(index).unwrap());
        if let DataType::String | DataType::Categorical(_, _) = self.series.borrow().dtype() {
            let v_trunc = &val[..val
                .char_indices()
                .take(str_lengths)
                .last()
                .map(|(i, c)| i + c.len_utf8())
                .unwrap_or(0)];
            if val == v_trunc {
                val
            } else {
                format!("{}…", v_trunc)
            }
        } else {
            val
        }
    }

    pub fn rechunk(&self, in_place: bool) -> Option<Self> {
        let series = self.series.borrow_mut().rechunk();
        if in_place {
            *self.series.borrow_mut() = series;
            None
        } else {
            Some(series.into())
        }
    }

    pub fn get_idx(&self, idx: usize) -> RbResult<Value> {
        Ok(Wrap(self.series.borrow().get(idx).map_err(RbPolarsErr::from)?).into_value())
    }

    pub fn bitand(&self, other: &RbSeries) -> RbResult<Self> {
        let out = self
            .series
            .borrow()
            .bitand(&other.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn bitor(&self, other: &RbSeries) -> RbResult<Self> {
        let out = self
            .series
            .borrow()
            .bitor(&other.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn bitxor(&self, other: &RbSeries) -> RbResult<Self> {
        let out = self
            .series
            .borrow()
            .bitxor(&other.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn chunk_lengths(&self) -> Vec<usize> {
        self.series.borrow().chunk_lengths().collect()
    }

    pub fn name(&self) -> String {
        self.series.borrow().name().into()
    }

    pub fn rename(&self, name: String) {
        self.series.borrow_mut().rename(&name);
    }

    pub fn dtype(&self) -> Value {
        Wrap(self.series.borrow().dtype().clone()).into_value()
    }

    pub fn inner_dtype(&self) -> Option<Value> {
        self.series
            .borrow()
            .dtype()
            .inner_dtype()
            .map(|dt| Wrap(dt.clone()).into_value())
    }

    pub fn set_sorted_flag(&self, descending: bool) -> Self {
        let mut out = self.series.borrow().clone();
        if descending {
            out.set_sorted_flag(IsSorted::Descending);
        } else {
            out.set_sorted_flag(IsSorted::Ascending)
        }
        out.into()
    }

    pub fn n_chunks(&self) -> usize {
        self.series.borrow().n_chunks()
    }

    pub fn append(&self, other: &RbSeries) -> RbResult<()> {
        let mut binding = self.series.borrow_mut();
        let res = binding.append(&other.series.borrow());
        if let Err(e) = res {
            Err(Error::new(exception::runtime_error(), e.to_string()))
        } else {
            Ok(())
        }
    }

    pub fn extend(&self, other: &RbSeries) -> RbResult<()> {
        self.series
            .borrow_mut()
            .extend(&other.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn new_from_index(&self, index: usize, length: usize) -> RbResult<Self> {
        if index >= self.series.borrow().len() {
            Err(Error::new(exception::arg_error(), "index is out of bounds"))
        } else {
            Ok(self.series.borrow().new_from_index(index, length).into())
        }
    }

    pub fn filter(&self, filter: &RbSeries) -> RbResult<Self> {
        let filter_series = &filter.series.borrow();
        if let Ok(ca) = filter_series.bool() {
            let series = self.series.borrow().filter(ca).unwrap();
            Ok(series.into())
        } else {
            Err(Error::new(
                exception::runtime_error(),
                "Expected a boolean mask".to_string(),
            ))
        }
    }

    pub fn sort(&self, descending: bool, nulls_last: bool, multithreaded: bool) -> RbResult<Self> {
        Ok(self
            .series
            .borrow_mut()
            .sort(
                SortOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last)
                    .with_multithreaded(multithreaded),
            )
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn value_counts(&self, sorted: bool) -> RbResult<RbDataFrame> {
        let df = self
            .series
            .borrow()
            .value_counts(true, sorted)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn take_with_series(&self, indices: &RbSeries) -> RbResult<Self> {
        let binding = indices.series.borrow();
        let idx = binding.idx().map_err(RbPolarsErr::from)?;
        let take = self.series.borrow().take(idx).map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(take))
    }

    pub fn null_count(&self) -> RbResult<usize> {
        Ok(self.series.borrow().null_count())
    }

    pub fn has_validity(&self) -> bool {
        self.series.borrow().has_validity()
    }

    pub fn sample_n(
        &self,
        n: usize,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .sample_n(n, with_replacement, shuffle, seed)
            .map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn sample_frac(
        &self,
        frac: f64,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .sample_frac(frac, with_replacement, shuffle, seed)
            .map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn equals(&self, other: &RbSeries, null_equal: bool, strict: bool) -> bool {
        if strict {
            self.series.borrow().eq(&other.series.borrow())
        } else if null_equal {
            self.series.borrow().equals_missing(&other.series.borrow())
        } else {
            self.series.borrow().equals(&other.series.borrow())
        }
    }

    pub fn not(&self) -> RbResult<Self> {
        let binding = self.series.borrow();
        let bool = binding.bool().map_err(RbPolarsErr::from)?;
        Ok((!bool).into_series().into())
    }

    pub fn to_s(&self) -> String {
        format!("{}", self.series.borrow())
    }

    pub fn len(&self) -> usize {
        self.series.borrow().len()
    }

    pub fn to_a(&self) -> Value {
        let series = &self.series.borrow();

        fn to_a_recursive(series: &Series) -> Value {
            let rblist = match series.dtype() {
                DataType::Boolean => RArray::from_iter(series.bool().unwrap()).into_value(),
                DataType::UInt8 => RArray::from_iter(series.u8().unwrap()).into_value(),
                DataType::UInt16 => RArray::from_iter(series.u16().unwrap()).into_value(),
                DataType::UInt32 => RArray::from_iter(series.u32().unwrap()).into_value(),
                DataType::UInt64 => RArray::from_iter(series.u64().unwrap()).into_value(),
                DataType::Int8 => RArray::from_iter(series.i8().unwrap()).into_value(),
                DataType::Int16 => RArray::from_iter(series.i16().unwrap()).into_value(),
                DataType::Int32 => RArray::from_iter(series.i32().unwrap()).into_value(),
                DataType::Int64 => RArray::from_iter(series.i64().unwrap()).into_value(),
                DataType::Float32 => RArray::from_iter(series.f32().unwrap()).into_value(),
                DataType::Float64 => RArray::from_iter(series.f64().unwrap()).into_value(),
                DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                    RArray::from_iter(series.categorical().unwrap().iter_str()).into_value()
                }
                DataType::Object(_, _) => {
                    let v = RArray::with_capacity(series.len());
                    for i in 0..series.len() {
                        let obj: Option<&ObjectValue> = series.get_object(i).map(|any| any.into());
                        match obj {
                            Some(val) => v.push(val.to_object()).unwrap(),
                            None => v.push(qnil()).unwrap(),
                        };
                    }
                    v.into_value()
                }
                DataType::List(_) => {
                    let v = RArray::new();
                    let ca = series.list().unwrap();
                    for opt_s in unsafe { ca.amortized_iter() } {
                        match opt_s {
                            None => {
                                v.push(qnil()).unwrap();
                            }
                            Some(s) => {
                                let rblst = to_a_recursive(s.as_ref());
                                v.push(rblst).unwrap();
                            }
                        }
                    }
                    v.into_value()
                }
                DataType::Array(_, _) => {
                    let v = RArray::new();
                    let ca = series.array().unwrap();
                    for opt_s in ca.amortized_iter() {
                        match opt_s {
                            None => {
                                v.push(qnil()).unwrap();
                            }
                            Some(s) => {
                                let rblst = to_a_recursive(s.as_ref());
                                v.push(rblst).unwrap();
                            }
                        }
                    }
                    v.into_value()
                }
                DataType::Date => {
                    let ca = series.date().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Time => {
                    let ca = series.time().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Datetime(_, _) => {
                    let ca = series.datetime().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Decimal(_, _) => {
                    let ca = series.decimal().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::String => {
                    let ca = series.str().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Struct(_) => {
                    let ca = series.struct_().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Duration(_) => {
                    let ca = series.duration().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Binary => {
                    let ca = series.binary().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Null => {
                    let null: Option<u8> = None;
                    let n = series.len();
                    let iter = std::iter::repeat(null).take(n);
                    use std::iter::{Repeat, Take};
                    struct NullIter {
                        iter: Take<Repeat<Option<u8>>>,
                        n: usize,
                    }
                    impl Iterator for NullIter {
                        type Item = Option<u8>;

                        fn next(&mut self) -> Option<Self::Item> {
                            self.iter.next()
                        }
                        fn size_hint(&self) -> (usize, Option<usize>) {
                            (self.n, Some(self.n))
                        }
                    }
                    impl ExactSizeIterator for NullIter {}

                    RArray::from_iter(NullIter { iter, n }).into_value()
                }
                DataType::Unknown(_) => {
                    panic!("to_a not implemented for unknown")
                }
                DataType::BinaryOffset => {
                    unreachable!()
                }
            };
            rblist
        }

        to_a_recursive(series)
    }

    pub fn clone(&self) -> Self {
        RbSeries::new(self.series.borrow().clone())
    }

    pub fn apply_lambda(
        &self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        skip_nulls: bool,
    ) -> RbResult<Self> {
        let series = &self.series.borrow();

        let output_type = output_type.map(|dt| dt.0);

        macro_rules! dispatch_apply {
            ($self:expr, $method:ident, $($args:expr),*) => {
                if matches!($self.dtype(), DataType::Object(_, _)) {
                    // let ca = $self.0.unpack::<ObjectType<ObjectValue>>().unwrap();
                    // ca.$method($($args),*)
                    todo!()
                } else {
                    apply_method_all_arrow_series2!(
                        $self,
                        $method,
                        $($args),*
                    )
                }

            }

        }

        if matches!(
            series.dtype(),
            DataType::Datetime(_, _)
                | DataType::Date
                | DataType::Duration(_)
                | DataType::Categorical(_, _)
                | DataType::Time
        ) || !skip_nulls
        {
            let mut avs = Vec::with_capacity(series.len());
            let iter = series.iter().map(|av| {
                let input = Wrap(av);
                call_lambda_and_extract::<_, Wrap<AnyValue>>(lambda, input)
                    .unwrap()
                    .0
            });
            avs.extend(iter);
            return Ok(Series::new(&self.name(), &avs).into());
        }

        let out = match output_type {
            Some(DataType::Int8) => {
                let ca: Int8Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Int16) => {
                let ca: Int16Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Int32) => {
                let ca: Int32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Int64) => {
                let ca: Int64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt8) => {
                let ca: UInt8Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt16) => {
                let ca: UInt16Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt32) => {
                let ca: UInt32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::UInt64) => {
                let ca: UInt64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Float32) => {
                let ca: Float32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Float64) => {
                let ca: Float64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_series()
            }
            Some(DataType::Boolean) => {
                let ca: BooleanChunked =
                    dispatch_apply!(series, apply_lambda_with_bool_out_type, lambda, 0, None)?;
                ca.into_series()
            }
            Some(DataType::Date) => {
                let ca: Int32Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_date().into_series()
            }
            Some(DataType::Datetime(tu, tz)) => {
                let ca: Int64Chunked = dispatch_apply!(
                    series,
                    apply_lambda_with_primitive_out_type,
                    lambda,
                    0,
                    None
                )?;
                ca.into_datetime(tu, tz).into_series()
            }
            Some(DataType::String) => {
                let ca = dispatch_apply!(series, apply_lambda_with_utf8_out_type, lambda, 0, None)?;

                ca.into_series()
            }
            Some(DataType::Object(_, _)) => {
                let ca =
                    dispatch_apply!(series, apply_lambda_with_object_out_type, lambda, 0, None)?;
                ca.into_series()
            }
            None => return dispatch_apply!(series, apply_lambda_unknown, lambda),

            _ => return dispatch_apply!(series, apply_lambda_unknown, lambda),
        };

        Ok(RbSeries::new(out))
    }

    pub fn zip_with(&self, mask: &RbSeries, other: &RbSeries) -> RbResult<Self> {
        let binding = mask.series.borrow();
        let mask = binding.bool().map_err(RbPolarsErr::from)?;
        let s = self
            .series
            .borrow()
            .zip_with(mask, &other.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s))
    }

    pub fn to_dummies(&self, sep: Option<String>, drop_first: bool) -> RbResult<RbDataFrame> {
        let df = self
            .series
            .borrow()
            .to_dummies(sep.as_deref(), drop_first)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn n_unique(&self) -> RbResult<usize> {
        let n = self.series.borrow().n_unique().map_err(RbPolarsErr::from)?;
        Ok(n)
    }

    pub fn floor(&self) -> RbResult<Self> {
        let s = self.series.borrow().floor().map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn shrink_to_fit(&self) {
        self.series.borrow_mut().shrink_to_fit();
    }

    pub fn dot(&self, other: &RbSeries) -> RbResult<f64> {
        let out = self
            .series
            .borrow()
            .dot(&other.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn skew(&self, bias: bool) -> RbResult<Option<f64>> {
        let out = self.series.borrow().skew(bias).map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn kurtosis(&self, fisher: bool, bias: bool) -> RbResult<Option<f64>> {
        let out = self
            .series
            .borrow()
            .kurtosis(fisher, bias)
            .map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn cast(&self, dtype: Wrap<DataType>, strict: bool) -> RbResult<Self> {
        let dtype = dtype.0;
        let out = if strict {
            self.series.borrow().strict_cast(&dtype)
        } else {
            self.series.borrow().cast(&dtype)
        };
        let out = out.map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn time_unit(&self) -> Option<String> {
        if let DataType::Datetime(tu, _) | DataType::Duration(tu) = self.series.borrow().dtype() {
            Some(
                match tu {
                    TimeUnit::Nanoseconds => "ns",
                    TimeUnit::Microseconds => "us",
                    TimeUnit::Milliseconds => "ms",
                }
                .to_string(),
            )
        } else {
            None
        }
    }
}

macro_rules! impl_set_with_mask {
    ($name:ident, $native:ty, $cast:ident, $variant:ident) => {
        fn $name(
            series: &Series,
            filter: &RbSeries,
            value: Option<$native>,
        ) -> PolarsResult<Series> {
            let binding = filter.series.borrow();
            let mask = binding.bool()?;
            let ca = series.$cast()?;
            let new = ca.set(mask, value)?;
            Ok(new.into_series())
        }

        impl RbSeries {
            pub fn $name(&self, filter: &RbSeries, value: Option<$native>) -> RbResult<Self> {
                let series =
                    $name(&self.series.borrow(), filter, value).map_err(RbPolarsErr::from)?;
                Ok(Self::new(series))
            }
        }
    };
}

// impl_set_with_mask!(set_with_mask_str, &str, utf8, Utf8);
impl_set_with_mask!(set_with_mask_f64, f64, f64, Float64);
impl_set_with_mask!(set_with_mask_f32, f32, f32, Float32);
impl_set_with_mask!(set_with_mask_u8, u8, u8, UInt8);
impl_set_with_mask!(set_with_mask_u16, u16, u16, UInt16);
impl_set_with_mask!(set_with_mask_u32, u32, u32, UInt32);
impl_set_with_mask!(set_with_mask_u64, u64, u64, UInt64);
impl_set_with_mask!(set_with_mask_i8, i8, i8, Int8);
impl_set_with_mask!(set_with_mask_i16, i16, i16, Int16);
impl_set_with_mask!(set_with_mask_i32, i32, i32, Int32);
impl_set_with_mask!(set_with_mask_i64, i64, i64, Int64);
impl_set_with_mask!(set_with_mask_bool, bool, bool, Boolean);

impl RbSeries {
    pub fn extend_constant(&self, value: Wrap<AnyValue>, n: usize) -> RbResult<Self> {
        Ok(self
            .series
            .borrow()
            .clone()
            .extend_constant(value.0, n)
            .map_err(RbPolarsErr::from)?
            .into())
    }
}

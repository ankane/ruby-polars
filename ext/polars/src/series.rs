use magnus::{exception, Error, IntoValue, RArray, Value, QNIL};
use polars::prelude::*;
use polars::series::IsSorted;
use std::cell::RefCell;

use crate::apply::series::{call_lambda_and_extract, ApplyLambda};
use crate::apply_method_all_arrow_series2;
use crate::conversion::*;
use crate::list_construction::rb_seq_to_list;
use crate::set::set_at_idx;
use crate::{RbDataFrame, RbPolarsErr, RbResult, RbValueError};

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

    pub fn is_sorted_flag(&self) -> bool {
        matches!(self.series.borrow().is_sorted_flag(), IsSorted::Ascending)
    }

    pub fn is_sorted_reverse_flag(&self) -> bool {
        matches!(self.series.borrow().is_sorted_flag(), IsSorted::Descending)
    }

    pub fn new_opt_bool(name: String, obj: RArray, strict: bool) -> RbResult<RbSeries> {
        let len = obj.len();
        let mut builder = BooleanChunkedBuilder::new(&name, len);

        unsafe {
            for item in obj.as_slice().iter() {
                if item.is_nil() {
                    builder.append_null()
                } else {
                    match item.try_convert::<bool>() {
                        Ok(val) => builder.append_value(val),
                        Err(e) => {
                            if strict {
                                return Err(e);
                            }
                            builder.append_null()
                        }
                    }
                }
            }
        }
        let ca = builder.finish();

        let s = ca.into_series();
        Ok(RbSeries::new(s))
    }
}

fn new_primitive<T>(name: &str, obj: RArray, strict: bool) -> RbResult<RbSeries>
where
    T: PolarsNumericType,
    ChunkedArray<T>: IntoSeries,
    T::Native: magnus::TryConvert,
{
    let len = obj.len();
    let mut builder = PrimitiveChunkedBuilder::<T>::new(name, len);

    unsafe {
        for item in obj.as_slice().iter() {
            if item.is_nil() {
                builder.append_null()
            } else {
                match item.try_convert::<T::Native>() {
                    Ok(val) => builder.append_value(val),
                    Err(e) => {
                        if strict {
                            return Err(e);
                        }
                        builder.append_null()
                    }
                }
            }
        }
    }
    let ca = builder.finish();

    let s = ca.into_series();
    Ok(RbSeries::new(s))
}

// Init with lists that can contain Nones
macro_rules! init_method_opt {
    ($name:ident, $type:ty, $native: ty) => {
        impl RbSeries {
            pub fn $name(name: String, obj: RArray, strict: bool) -> RbResult<Self> {
                new_primitive::<$type>(&name, obj, strict)
            }
        }
    };
}

init_method_opt!(new_opt_u8, UInt8Type, u8);
init_method_opt!(new_opt_u16, UInt16Type, u16);
init_method_opt!(new_opt_u32, UInt32Type, u32);
init_method_opt!(new_opt_u64, UInt64Type, u64);
init_method_opt!(new_opt_i8, Int8Type, i8);
init_method_opt!(new_opt_i16, Int16Type, i16);
init_method_opt!(new_opt_i32, Int32Type, i32);
init_method_opt!(new_opt_i64, Int64Type, i64);
init_method_opt!(new_opt_f32, Float32Type, f32);
init_method_opt!(new_opt_f64, Float64Type, f64);

impl RbSeries {
    pub fn new_str(name: String, val: Wrap<Utf8Chunked>, _strict: bool) -> Self {
        let mut s = val.0.into_series();
        s.rename(&name);
        RbSeries::new(s)
    }

    pub fn new_binary(name: String, val: Wrap<BinaryChunked>, _strict: bool) -> Self {
        let mut s = val.0.into_series();
        s.rename(&name);
        RbSeries::new(s)
    }

    pub fn new_object(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let val = val
            .each()
            .map(|v| v.map(ObjectValue::from))
            .collect::<RbResult<Vec<ObjectValue>>>()?;
        let s = ObjectChunked::<ObjectValue>::new_from_vec(&name, val).into_series();
        Ok(s.into())
    }

    pub fn new_list(name: String, seq: Value, dtype: Wrap<DataType>) -> RbResult<Self> {
        rb_seq_to_list(&name, seq, &dtype.0).map(|s| s.into())
    }

    pub fn estimated_size(&self) -> usize {
        self.series.borrow().estimated_size()
    }

    pub fn get_fmt(&self, index: usize, str_lengths: usize) -> String {
        let val = format!("{}", self.series.borrow().get(index).unwrap());
        if let DataType::Utf8 | DataType::Categorical(_) = self.series.borrow().dtype() {
            let v_trunc = &val[..val
                .char_indices()
                .take(str_lengths)
                .last()
                .map(|(i, c)| i + c.len_utf8())
                .unwrap_or(0)];
            if val == v_trunc {
                val
            } else {
                format!("{}...", v_trunc)
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

    pub fn set_sorted(&self, reverse: bool) -> Self {
        let mut out = self.series.borrow().clone();
        if reverse {
            out.set_sorted_flag(IsSorted::Descending);
        } else {
            out.set_sorted_flag(IsSorted::Ascending)
        }
        out.into()
    }

    pub fn mean(&self) -> Option<f64> {
        match self.series.borrow().dtype() {
            DataType::Boolean => {
                let s = self.series.borrow().cast(&DataType::UInt8).unwrap();
                s.mean()
            }
            _ => self.series.borrow().mean(),
        }
    }

    pub fn max(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .max_as_series()
                .get(0)
                .map_err(RbPolarsErr::from)?,
        )
        .into_value())
    }

    pub fn min(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .min_as_series()
                .get(0)
                .map_err(RbPolarsErr::from)?,
        )
        .into_value())
    }

    pub fn sum(&self) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .sum_as_series()
                .get(0)
                .map_err(RbPolarsErr::from)?,
        )
        .into_value())
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

    pub fn add(&self, other: &RbSeries) -> Self {
        (&*self.series.borrow() + &*other.series.borrow()).into()
    }

    pub fn sub(&self, other: &RbSeries) -> Self {
        (&*self.series.borrow() - &*other.series.borrow()).into()
    }

    pub fn mul(&self, other: &RbSeries) -> Self {
        (&*self.series.borrow() * &*other.series.borrow()).into()
    }

    pub fn div(&self, other: &RbSeries) -> Self {
        (&*self.series.borrow() / &*other.series.borrow()).into()
    }

    pub fn rem(&self, other: &RbSeries) -> Self {
        (&*self.series.borrow() % &*other.series.borrow()).into()
    }

    pub fn sort(&self, reverse: bool) -> Self {
        (self.series.borrow_mut().sort(reverse)).into()
    }

    pub fn value_counts(&self, sorted: bool) -> RbResult<RbDataFrame> {
        let df = self
            .series
            .borrow()
            .value_counts(true, sorted)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn arg_min(&self) -> Option<usize> {
        self.series.borrow().arg_min()
    }

    pub fn arg_max(&self) -> Option<usize> {
        self.series.borrow().arg_max()
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

    pub fn series_equal(&self, other: &RbSeries, null_equal: bool, strict: bool) -> bool {
        if strict {
            self.series.borrow().eq(&other.series.borrow())
        } else if null_equal {
            self.series
                .borrow()
                .series_equal_missing(&other.series.borrow())
        } else {
            self.series.borrow().series_equal(&other.series.borrow())
        }
    }

    pub fn eq(&self, rhs: &RbSeries) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .equal(&*rhs.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(Self::new(s.into_series()))
    }

    pub fn neq(&self, rhs: &RbSeries) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .not_equal(&*rhs.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(Self::new(s.into_series()))
    }

    pub fn gt(&self, rhs: &RbSeries) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .gt(&*rhs.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(Self::new(s.into_series()))
    }

    pub fn gt_eq(&self, rhs: &RbSeries) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .gt_eq(&*rhs.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(Self::new(s.into_series()))
    }

    pub fn lt(&self, rhs: &RbSeries) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .lt(&*rhs.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(Self::new(s.into_series()))
    }

    pub fn lt_eq(&self, rhs: &RbSeries) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .lt_eq(&*rhs.series.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(Self::new(s.into_series()))
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

        fn to_list_recursive(series: &Series) -> Value {
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
                DataType::Categorical(_) => {
                    RArray::from_iter(series.categorical().unwrap().iter_str()).into_value()
                }
                DataType::Object(_) => {
                    let v = RArray::with_capacity(series.len());
                    for i in 0..series.len() {
                        let obj: Option<&ObjectValue> = series.get_object(i).map(|any| any.into());
                        match obj {
                            Some(val) => v.push(val.to_object()).unwrap(),
                            None => v.push(QNIL).unwrap(),
                        };
                    }
                    v.into_value()
                }
                DataType::Date => {
                    let a = RArray::with_capacity(series.len());
                    for v in series.iter() {
                        a.push::<Value>(Wrap(v).into_value()).unwrap();
                    }
                    return a.into_value();
                }
                DataType::Datetime(_, _) => {
                    let a = RArray::with_capacity(series.len());
                    for v in series.iter() {
                        a.push::<Value>(Wrap(v).into_value()).unwrap();
                    }
                    return a.into_value();
                }
                DataType::Utf8 => {
                    let ca = series.utf8().unwrap();
                    return RArray::from_iter(ca).into_value();
                }
                DataType::Duration(_) => {
                    let ca = series.duration().unwrap();
                    return Wrap(ca).into_value();
                }
                DataType::Binary => {
                    let a = RArray::with_capacity(series.len());
                    for v in series.iter() {
                        a.push::<Value>(Wrap(v).into_value()).unwrap();
                    }
                    return a.into_value();
                }
                DataType::Null | DataType::Unknown => {
                    panic!("to_a not implemented for null/unknown")
                }
                _ => todo!(),
            };
            rblist
        }

        to_list_recursive(series)
    }

    pub fn median(&self) -> Option<f64> {
        match self.series.borrow().dtype() {
            DataType::Boolean => {
                let s = self.series.borrow().cast(&DataType::UInt8).unwrap();
                s.median()
            }
            _ => self.series.borrow().median(),
        }
    }

    pub fn quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> RbResult<Value> {
        Ok(Wrap(
            self.series
                .borrow()
                .quantile_as_series(quantile, interpolation.0)
                .map_err(|_| RbValueError::new_err("invalid quantile".into()))?
                .get(0)
                .unwrap_or(AnyValue::Null),
        )
        .into_value())
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
                if matches!($self.dtype(), DataType::Object(_)) {
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
                | DataType::Categorical(_)
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
            Some(DataType::Utf8) => {
                let ca = dispatch_apply!(series, apply_lambda_with_utf8_out_type, lambda, 0, None)?;

                ca.into_series()
            }
            Some(DataType::Object(_)) => {
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

    pub fn to_dummies(&self, sep: Option<String>) -> RbResult<RbDataFrame> {
        let df = self
            .series
            .borrow()
            .to_dummies(sep.as_deref())
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn peak_max(&self) -> Self {
        self.series.borrow().peak_max().into_series().into()
    }

    pub fn peak_min(&self) -> Self {
        self.series.borrow().peak_min().into_series().into()
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

    pub fn dot(&self, other: &RbSeries) -> Option<f64> {
        self.series.borrow().dot(&other.series.borrow())
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

    pub fn set_at_idx(&self, idx: &RbSeries, values: &RbSeries) -> RbResult<()> {
        let mut s = self.series.borrow_mut();
        match set_at_idx(s.clone(), &idx.series.borrow(), &values.series.borrow()) {
            Ok(out) => {
                *s = out;
                Ok(())
            }
            Err(e) => Err(RbPolarsErr::from(e)),
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

macro_rules! impl_arithmetic {
    ($name:ident, $type:ty, $operand:tt) => {
        impl RbSeries {
            pub fn $name(&self, other: $type) -> RbResult<Self> {
                Ok(RbSeries::new(&*self.series.borrow() $operand other))
            }
        }
    };
}

impl_arithmetic!(add_u8, u8, +);
impl_arithmetic!(add_u16, u16, +);
impl_arithmetic!(add_u32, u32, +);
impl_arithmetic!(add_u64, u64, +);
impl_arithmetic!(add_i8, i8, +);
impl_arithmetic!(add_i16, i16, +);
impl_arithmetic!(add_i32, i32, +);
impl_arithmetic!(add_i64, i64, +);
impl_arithmetic!(add_datetime, i64, +);
impl_arithmetic!(add_duration, i64, +);
impl_arithmetic!(add_f32, f32, +);
impl_arithmetic!(add_f64, f64, +);
impl_arithmetic!(sub_u8, u8, -);
impl_arithmetic!(sub_u16, u16, -);
impl_arithmetic!(sub_u32, u32, -);
impl_arithmetic!(sub_u64, u64, -);
impl_arithmetic!(sub_i8, i8, -);
impl_arithmetic!(sub_i16, i16, -);
impl_arithmetic!(sub_i32, i32, -);
impl_arithmetic!(sub_i64, i64, -);
impl_arithmetic!(sub_datetime, i64, -);
impl_arithmetic!(sub_duration, i64, -);
impl_arithmetic!(sub_f32, f32, -);
impl_arithmetic!(sub_f64, f64, -);
impl_arithmetic!(div_u8, u8, /);
impl_arithmetic!(div_u16, u16, /);
impl_arithmetic!(div_u32, u32, /);
impl_arithmetic!(div_u64, u64, /);
impl_arithmetic!(div_i8, i8, /);
impl_arithmetic!(div_i16, i16, /);
impl_arithmetic!(div_i32, i32, /);
impl_arithmetic!(div_i64, i64, /);
impl_arithmetic!(div_f32, f32, /);
impl_arithmetic!(div_f64, f64, /);
impl_arithmetic!(mul_u8, u8, *);
impl_arithmetic!(mul_u16, u16, *);
impl_arithmetic!(mul_u32, u32, *);
impl_arithmetic!(mul_u64, u64, *);
impl_arithmetic!(mul_i8, i8, *);
impl_arithmetic!(mul_i16, i16, *);
impl_arithmetic!(mul_i32, i32, *);
impl_arithmetic!(mul_i64, i64, *);
impl_arithmetic!(mul_f32, f32, *);
impl_arithmetic!(mul_f64, f64, *);
impl_arithmetic!(rem_u8, u8, %);
impl_arithmetic!(rem_u16, u16, %);
impl_arithmetic!(rem_u32, u32, %);
impl_arithmetic!(rem_u64, u64, %);
impl_arithmetic!(rem_i8, i8, %);
impl_arithmetic!(rem_i16, i16, %);
impl_arithmetic!(rem_i32, i32, %);
impl_arithmetic!(rem_i64, i64, %);
impl_arithmetic!(rem_f32, f32, %);
impl_arithmetic!(rem_f64, f64, %);

macro_rules! impl_eq_num {
    ($name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(&self, rhs: $type) -> RbResult<Self> {
                let s = self.series.borrow().equal(rhs).map_err(RbPolarsErr::from)?;
                Ok(RbSeries::new(s.into_series()))
            }
        }
    };
}

impl_eq_num!(eq_u8, u8);
impl_eq_num!(eq_u16, u16);
impl_eq_num!(eq_u32, u32);
impl_eq_num!(eq_u64, u64);
impl_eq_num!(eq_i8, i8);
impl_eq_num!(eq_i16, i16);
impl_eq_num!(eq_i32, i32);
impl_eq_num!(eq_i64, i64);
impl_eq_num!(eq_f32, f32);
impl_eq_num!(eq_f64, f64);

macro_rules! impl_neq_num {
    ($name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(&self, rhs: $type) -> RbResult<Self> {
                let s = self
                    .series
                    .borrow()
                    .not_equal(rhs)
                    .map_err(RbPolarsErr::from)?;
                Ok(RbSeries::new(s.into_series()))
            }
        }
    };
}

impl_neq_num!(neq_u8, u8);
impl_neq_num!(neq_u16, u16);
impl_neq_num!(neq_u32, u32);
impl_neq_num!(neq_u64, u64);
impl_neq_num!(neq_i8, i8);
impl_neq_num!(neq_i16, i16);
impl_neq_num!(neq_i32, i32);
impl_neq_num!(neq_i64, i64);
impl_neq_num!(neq_f32, f32);
impl_neq_num!(neq_f64, f64);

macro_rules! impl_gt_num {
    ($name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(&self, rhs: $type) -> RbResult<Self> {
                let s = self.series.borrow().gt(rhs).map_err(RbPolarsErr::from)?;
                Ok(RbSeries::new(s.into_series()))
            }
        }
    };
}

impl_gt_num!(gt_u8, u8);
impl_gt_num!(gt_u16, u16);
impl_gt_num!(gt_u32, u32);
impl_gt_num!(gt_u64, u64);
impl_gt_num!(gt_i8, i8);
impl_gt_num!(gt_i16, i16);
impl_gt_num!(gt_i32, i32);
impl_gt_num!(gt_i64, i64);
impl_gt_num!(gt_f32, f32);
impl_gt_num!(gt_f64, f64);

macro_rules! impl_gt_eq_num {
    ($name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(&self, rhs: $type) -> RbResult<Self> {
                let s = self.series.borrow().gt_eq(rhs).map_err(RbPolarsErr::from)?;
                Ok(RbSeries::new(s.into_series()))
            }
        }
    };
}

impl_gt_eq_num!(gt_eq_u8, u8);
impl_gt_eq_num!(gt_eq_u16, u16);
impl_gt_eq_num!(gt_eq_u32, u32);
impl_gt_eq_num!(gt_eq_u64, u64);
impl_gt_eq_num!(gt_eq_i8, i8);
impl_gt_eq_num!(gt_eq_i16, i16);
impl_gt_eq_num!(gt_eq_i32, i32);
impl_gt_eq_num!(gt_eq_i64, i64);
impl_gt_eq_num!(gt_eq_f32, f32);
impl_gt_eq_num!(gt_eq_f64, f64);

macro_rules! impl_lt_num {
    ($name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(&self, rhs: $type) -> RbResult<RbSeries> {
                let s = self.series.borrow().lt(rhs).map_err(RbPolarsErr::from)?;
                Ok(RbSeries::new(s.into_series()))
            }
        }
    };
}

impl_lt_num!(lt_u8, u8);
impl_lt_num!(lt_u16, u16);
impl_lt_num!(lt_u32, u32);
impl_lt_num!(lt_u64, u64);
impl_lt_num!(lt_i8, i8);
impl_lt_num!(lt_i16, i16);
impl_lt_num!(lt_i32, i32);
impl_lt_num!(lt_i64, i64);
impl_lt_num!(lt_f32, f32);
impl_lt_num!(lt_f64, f64);

macro_rules! impl_lt_eq_num {
    ($name:ident, $type:ty) => {
        impl RbSeries {
            pub fn $name(&self, rhs: $type) -> RbResult<Self> {
                let s = self.series.borrow().lt_eq(rhs).map_err(RbPolarsErr::from)?;
                Ok(RbSeries::new(s.into_series()))
            }
        }
    };
}

impl_lt_eq_num!(lt_eq_u8, u8);
impl_lt_eq_num!(lt_eq_u16, u16);
impl_lt_eq_num!(lt_eq_u32, u32);
impl_lt_eq_num!(lt_eq_u64, u64);
impl_lt_eq_num!(lt_eq_i8, i8);
impl_lt_eq_num!(lt_eq_i16, i16);
impl_lt_eq_num!(lt_eq_i32, i32);
impl_lt_eq_num!(lt_eq_i64, i64);
impl_lt_eq_num!(lt_eq_f32, f32);
impl_lt_eq_num!(lt_eq_f64, f64);

impl RbSeries {
    pub fn eq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .equal(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn neq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .not_equal(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn gt_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .gt(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn gt_eq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .gt_eq(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn lt_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .lt(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }

    pub fn lt_eq_str(&self, rhs: String) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .lt_eq(rhs.as_str())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s.into_series()))
    }
}

pub fn to_series_collection(rs: RArray) -> RbResult<Vec<Series>> {
    let mut series = Vec::new();
    for item in rs.each() {
        series.push(item?.try_convert::<&RbSeries>()?.series.borrow().clone());
    }
    Ok(series)
}

pub fn to_rbseries_collection(s: Vec<Series>) -> RArray {
    RArray::from_iter(s.into_iter().map(RbSeries::new))
}

impl RbSeries {
    pub fn new_opt_date(name: String, values: RArray, _strict: Option<bool>) -> RbResult<Self> {
        let len = values.len();
        let mut builder = PrimitiveChunkedBuilder::<Int32Type>::new(&name, len);
        for item in values.each() {
            let v = item?;
            if v.is_nil() {
                builder.append_null();
            } else {
                // convert to DateTime for UTC
                let v = v
                    .funcall::<_, _, Value>("to_datetime", ())?
                    .funcall::<_, _, Value>("to_time", ())?
                    .funcall::<_, _, i64>("to_i", ())?;

                // TODO use strict
                builder.append_value((v / 86400) as i32);
            }
        }
        let ca: ChunkedArray<Int32Type> = builder.finish();
        Ok(ca.into_date().into_series().into())
    }

    pub fn new_opt_datetime(name: String, values: RArray, _strict: Option<bool>) -> RbResult<Self> {
        let len = values.len();
        let mut builder = PrimitiveChunkedBuilder::<Int64Type>::new(&name, len);
        for item in values.each() {
            let v = item?;
            if v.is_nil() {
                builder.append_null();
            } else {
                let sec: i64 = v.funcall("to_i", ())?;
                let nsec: i64 = v.funcall("nsec", ())?;
                // TODO use strict
                builder.append_value(sec * 1_000_000_000 + nsec);
            }
        }
        let ca: ChunkedArray<Int64Type> = builder.finish();
        Ok(ca
            .into_datetime(TimeUnit::Nanoseconds, None)
            .into_series()
            .into())
    }
}

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

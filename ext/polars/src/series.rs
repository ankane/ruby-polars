use magnus::exception::arg_error;
use magnus::{Error, RArray, Value};
use polars::prelude::*;
use polars::series::IsSorted;
use std::cell::RefCell;

use crate::conversion::*;
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
        matches!(self.series.borrow().is_sorted(), IsSorted::Ascending)
    }

    pub fn is_sorted_reverse_flag(&self) -> bool {
        matches!(self.series.borrow().is_sorted(), IsSorted::Descending)
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
    pub fn new_str(name: String, val: RArray, _strict: bool) -> RbResult<Self> {
        let v = val.try_convert::<Vec<Option<String>>>()?;
        let mut s = Utf8Chunked::new(&name, v).into_series();
        s.rename(&name);
        Ok(RbSeries::new(s))
    }

    pub fn estimated_size(&self) -> usize {
        self.series.borrow().estimated_size()
    }

    pub fn get_fmt(&self, index: usize, str_lengths: usize) -> String {
        let val = format!("{}", self.series.borrow().get(index));
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

    pub fn get_idx(&self, idx: usize) -> Value {
        Wrap(self.series.borrow().get(idx)).into()
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

    pub fn dtype(&self) -> String {
        self.series.borrow().dtype().to_string()
    }

    pub fn inner_dtype(&self) -> Option<String> {
        self.series
            .borrow()
            .dtype()
            .inner_dtype()
            .map(|dt| dt.to_string())
    }

    pub fn set_sorted(&self, reverse: bool) -> Self {
        let mut out = self.series.borrow().clone();
        if reverse {
            out.set_sorted(IsSorted::Descending);
        } else {
            out.set_sorted(IsSorted::Ascending)
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

    pub fn max(&self) -> Value {
        Wrap(self.series.borrow().max_as_series().get(0)).into()
    }

    pub fn min(&self) -> Value {
        Wrap(self.series.borrow().min_as_series().get(0)).into()
    }

    pub fn sum(&self) -> Value {
        Wrap(self.series.borrow().sum_as_series().get(0)).into()
    }

    pub fn n_chunks(&self) -> usize {
        self.series.borrow().n_chunks()
    }

    pub fn append(&self, other: &RbSeries) -> RbResult<()> {
        let mut binding = self.series.borrow_mut();
        let res = binding.append(&other.series.borrow());
        if let Err(e) = res {
            Err(Error::runtime_error(e.to_string()))
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
            Err(Error::new(arg_error(), "index is out of bounds"))
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
            Err(Error::runtime_error("Expected a boolean mask".to_string()))
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

    pub fn to_a(&self) -> RArray {
        let series = self.series.borrow();
        if let Ok(s) = series.f32() {
            s.into_iter().collect()
        } else if let Ok(s) = series.f64() {
            s.into_iter().collect()
        } else if let Ok(s) = series.i8() {
            s.into_iter().collect()
        } else if let Ok(s) = series.i16() {
            s.into_iter().collect()
        } else if let Ok(s) = series.i32() {
            s.into_iter().collect()
        } else if let Ok(s) = series.i64() {
            s.into_iter().collect()
        } else if let Ok(s) = series.u8() {
            s.into_iter().collect()
        } else if let Ok(s) = series.u16() {
            s.into_iter().collect()
        } else if let Ok(s) = series.u32() {
            s.into_iter().collect()
        } else if let Ok(s) = series.u64() {
            s.into_iter().collect()
        } else if let Ok(s) = series.bool() {
            s.into_iter().collect()
        } else if let Ok(s) = series.utf8() {
            s.into_iter().collect()
        } else {
            unimplemented!();
        }
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
                .get(0),
        )
        .into())
    }

    pub fn clone(&self) -> Self {
        RbSeries::new(self.series.borrow().clone())
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

    pub fn to_dummies(&self) -> RbResult<RbDataFrame> {
        let df = self
            .series
            .borrow()
            .to_dummies()
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

    // dispatch dynamically in future?

    pub fn cumsum(&self, reverse: bool) -> Self {
        self.series.borrow().cumsum(reverse).into()
    }

    pub fn cummax(&self, reverse: bool) -> Self {
        self.series.borrow().cummax(reverse).into()
    }

    pub fn cummin(&self, reverse: bool) -> Self {
        self.series.borrow().cummin(reverse).into()
    }

    pub fn cumprod(&self, reverse: bool) -> Self {
        self.series.borrow().cumprod(reverse).into()
    }

    pub fn slice(&self, offset: i64, length: usize) -> Self {
        let series = self.series.borrow().slice(offset, length);
        series.into()
    }

    pub fn ceil(&self) -> RbResult<Self> {
        let s = self.series.borrow().ceil().map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn round(&self, decimals: u32) -> RbResult<Self> {
        let s = self
            .series
            .borrow()
            .round(decimals)
            .map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }
}

pub fn to_rbseries_collection(s: Vec<Series>) -> Vec<RbSeries> {
    s.into_iter().map(RbSeries::new).collect()
}

use magnus::{Error, IntoValue, RArray, Ruby, Value, value::ReprValue};
use polars::prelude::*;
use polars::series::IsSorted;
use polars_core::utils::flatten::flatten_series;

use crate::conversion::*;
use crate::exceptions::RbIndexError;
use crate::rb_modules;
use crate::utils::EnterPolarsExt;
use crate::{RbDataFrame, RbErr, RbPolarsErr, RbResult, RbSeries};

impl RbSeries {
    pub fn struct_unnest(rb: &Ruby, self_: &Self) -> RbResult<RbDataFrame> {
        rb.enter_polars_df(|| Ok(self_.series.read().struct_()?.clone().unnest()))
    }

    pub fn struct_fields(&self) -> RbResult<Vec<String>> {
        let binding = self.series.read();
        let ca = binding.struct_().map_err(RbPolarsErr::from)?;
        Ok(ca
            .struct_fields()
            .iter()
            .map(|s| s.name().to_string())
            .collect())
    }

    pub fn is_sorted_ascending_flag(&self) -> bool {
        matches!(self.series.read().is_sorted_flag(), IsSorted::Ascending)
    }

    pub fn is_sorted_descending_flag(&self) -> bool {
        matches!(self.series.read().is_sorted_flag(), IsSorted::Descending)
    }

    pub fn can_fast_explode_flag(&self) -> bool {
        match self.series.read().list() {
            Err(_) => false,
            Ok(list) => list._can_fast_explode(),
        }
    }

    pub fn cat_uses_lexical_ordering(&self) -> RbResult<bool> {
        Ok(true)
    }

    pub fn cat_is_local(&self) -> RbResult<bool> {
        Ok(false)
    }

    pub fn cat_to_local(&self) -> RbResult<Self> {
        Ok(self.clone())
    }

    pub fn estimated_size(&self) -> usize {
        self.series.read().estimated_size()
    }

    pub fn get_fmt(&self, index: usize, str_lengths: usize) -> String {
        let val = format!("{}", self.series.read().get(index).unwrap());
        if let DataType::String | DataType::Categorical(_, _) = self.series.read().dtype() {
            let v_trunc = &val[..val
                .char_indices()
                .take(str_lengths)
                .last()
                .map(|(i, c)| i + c.len_utf8())
                .unwrap_or(0)];
            if val == v_trunc {
                val
            } else {
                format!("{v_trunc}…")
            }
        } else {
            val
        }
    }

    pub fn rechunk(rb: &Ruby, self_: &Self, in_place: bool) -> RbResult<Option<Self>> {
        let series = rb.enter_polars_ok(|| self_.series.read().rechunk())?;
        if in_place {
            *self_.series.write() = series;
            Ok(None)
        } else {
            Ok(Some(series.into()))
        }
    }

    pub fn get_index(ruby: &Ruby, self_: &Self, index: usize) -> RbResult<Value> {
        let binding = self_.series.read();
        let av = match binding.get(index) {
            Ok(v) => v,
            Err(PolarsError::OutOfBounds(err)) => {
                return Err(RbIndexError::new_err(err.to_string()));
            }
            Err(e) => return Err(RbPolarsErr::from(e).into()),
        };

        match av {
            AnyValue::List(s) | AnyValue::Array(s, _) => {
                let rbseries = RbSeries::new(s);
                rb_modules::pl_utils().funcall("wrap_s", (rbseries,))
            }
            _ => Ok(Wrap(av).into_value_with(ruby)),
        }
    }

    pub fn get_index_signed(ruby: &Ruby, self_: &Self, index: isize) -> RbResult<Value> {
        let index = if index < 0 {
            match self_.len().checked_sub(index.unsigned_abs()) {
                Some(v) => v,
                None => {
                    return Err(RbIndexError::new_err(
                        polars_err!(oob = index, self_.len()).to_string(),
                    ));
                }
            }
        } else {
            usize::try_from(index).unwrap()
        };
        Self::get_index(ruby, self_, index)
    }

    pub fn bitand(&self, other: &RbSeries) -> RbResult<Self> {
        let out = (&*self.series.read() & &*other.series.read()).map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn bitor(&self, other: &RbSeries) -> RbResult<Self> {
        let out = (&*self.series.read() | &*other.series.read()).map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn bitxor(&self, other: &RbSeries) -> RbResult<Self> {
        let out = (&*self.series.read() ^ &*other.series.read()).map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn chunk_lengths(&self) -> Vec<usize> {
        self.series.read().chunk_lengths().collect()
    }

    pub fn name(&self) -> String {
        self.series.read().name().to_string()
    }

    pub fn rename(&self, name: String) {
        self.series.write().rename(name.into());
    }

    pub fn dtype(ruby: &Ruby, self_: &Self) -> Value {
        Wrap(self_.series.read().dtype().clone()).into_value_with(ruby)
    }

    pub fn inner_dtype(ruby: &Ruby, self_: &Self) -> Option<Value> {
        self_
            .series
            .read()
            .dtype()
            .inner_dtype()
            .map(|dt| Wrap(dt.clone()).into_value_with(ruby))
    }

    pub fn set_sorted_flag(&self, descending: bool) -> Self {
        let mut out = self.series.read().clone();
        if descending {
            out.set_sorted_flag(IsSorted::Descending);
        } else {
            out.set_sorted_flag(IsSorted::Ascending)
        }
        out.into()
    }

    pub fn n_chunks(&self) -> usize {
        self.series.read().n_chunks()
    }

    pub fn append(rb: &Ruby, self_: &Self, other: &RbSeries) -> RbResult<()> {
        rb.enter_polars(|| {
            // Prevent self-append deadlocks.
            let other = other.series.read().clone();
            let mut s = self_.series.write();
            s.append(&other)?;
            PolarsResult::Ok(())
        })
    }

    pub fn extend(rb: &Ruby, self_: &Self, other: &RbSeries) -> RbResult<()> {
        rb.enter_polars(|| {
            // Prevent self-extend deadlocks.
            let other = other.series.read().clone();
            let mut s = self_.series.write();
            s.extend(&other)?;
            PolarsResult::Ok(())
        })
    }

    pub fn new_from_index(
        ruby: &Ruby,
        self_: &Self,
        index: usize,
        length: usize,
    ) -> RbResult<Self> {
        if index >= self_.series.read().len() {
            Err(Error::new(
                ruby.exception_arg_error(),
                "index is out of bounds",
            ))
        } else {
            Ok(self_.series.read().new_from_index(index, length).into())
        }
    }

    pub fn filter(ruby: &Ruby, self_: &Self, filter: &RbSeries) -> RbResult<Self> {
        let filter_series = &filter.series.read();
        if let Ok(ca) = filter_series.bool() {
            let series = self_.series.read().filter(ca).unwrap();
            Ok(series.into())
        } else {
            Err(Error::new(
                ruby.exception_runtime_error(),
                "Expected a boolean mask".to_string(),
            ))
        }
    }

    pub fn sort(&self, descending: bool, nulls_last: bool, multithreaded: bool) -> RbResult<Self> {
        Ok(self
            .series
            .write()
            .sort(
                SortOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last)
                    .with_multithreaded(multithreaded),
            )
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn value_counts(
        &self,
        sort: bool,
        parallel: bool,
        name: String,
        normalize: bool,
    ) -> RbResult<RbDataFrame> {
        let out = self
            .series
            .read()
            .value_counts(sort, parallel, name.into(), normalize)
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn slice(&self, offset: i64, length: Option<usize>) -> Self {
        let length = length.unwrap_or_else(|| self.series.read().len());
        self.series.read().slice(offset, length).into()
    }

    pub fn take_with_series(&self, indices: &RbSeries) -> RbResult<Self> {
        let binding = indices.series.read();
        let idx = binding.idx().map_err(RbPolarsErr::from)?;
        let take = self.series.read().take(idx).map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(take))
    }

    pub fn null_count(&self) -> RbResult<usize> {
        Ok(self.series.read().null_count())
    }

    pub fn has_nulls(&self) -> bool {
        self.series.read().has_nulls()
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
            .read()
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
            .read()
            .sample_frac(frac, with_replacement, shuffle, seed)
            .map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn equals(
        &self,
        other: &RbSeries,
        check_dtypes: bool,
        check_names: bool,
        null_equal: bool,
    ) -> bool {
        if check_dtypes && (self.series.read().dtype() != other.series.read().dtype()) {
            return false;
        }
        if check_names && (self.series.read().name() != other.series.read().name()) {
            return false;
        }
        if null_equal {
            self.series.read().equals_missing(&other.series.read())
        } else {
            self.series.read().equals(&other.series.read())
        }
    }

    pub fn not_(&self) -> RbResult<Self> {
        let binding = self.series.read();
        let bool = binding.bool().map_err(RbPolarsErr::from)?;
        Ok((!bool).into_series().into())
    }

    pub fn shrink_dtype(&self) -> RbResult<Self> {
        self.series
            .read()
            .shrink_type()
            .map(Into::into)
            .map_err(RbPolarsErr::from)
            .map_err(RbErr::from)
    }

    pub fn str_to_decimal_infer(&self, inference_length: usize) -> RbResult<Self> {
        let s = self.series.read();
        let ca = s.str().map_err(RbPolarsErr::from)?;
        ca.to_decimal_infer(inference_length)
            .map(Into::into)
            .map_err(RbPolarsErr::from)
            .map_err(RbErr::from)
    }

    pub fn str_json_decode(&self, infer_schema_length: Option<usize>) -> RbResult<Self> {
        let lock = self.series.read();
        lock.str()
            .map_err(RbPolarsErr::from)?
            .json_decode(None, infer_schema_length)
            .map(|s| s.with_name(lock.name().clone()))
            .map(Into::into)
            .map_err(RbPolarsErr::from)
            .map_err(RbErr::from)
    }

    pub fn to_s(&self) -> String {
        format!("{}", self.series.read())
    }

    pub fn len(&self) -> usize {
        self.series.read().len()
    }

    pub fn clone(&self) -> Self {
        RbSeries::new(self.series.read().clone())
    }

    pub fn zip_with(&self, mask: &RbSeries, other: &RbSeries) -> RbResult<Self> {
        let binding = mask.series.read();
        let mask = binding.bool().map_err(RbPolarsErr::from)?;
        let s = self
            .series
            .read()
            .zip_with(mask, &other.series.read())
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s))
    }

    pub fn to_dummies(
        &self,
        sep: Option<String>,
        drop_first: bool,
        drop_nulls: bool,
    ) -> RbResult<RbDataFrame> {
        let df = self
            .series
            .read()
            .to_dummies(sep.as_deref(), drop_first, drop_nulls)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn n_unique(&self) -> RbResult<usize> {
        let n = self.series.read().n_unique().map_err(RbPolarsErr::from)?;
        Ok(n)
    }

    pub fn floor(&self) -> RbResult<Self> {
        let s = self.series.read().floor().map_err(RbPolarsErr::from)?;
        Ok(s.into())
    }

    pub fn shrink_to_fit(&self) {
        self.series.write().shrink_to_fit();
    }

    pub fn dot(&self, other: &RbSeries) -> RbResult<f64> {
        let out = self
            .series
            .read()
            .dot(&other.series.read())
            .map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn skew(&self, bias: bool) -> RbResult<Option<f64>> {
        let out = self.series.read().skew(bias).map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn kurtosis(&self, fisher: bool, bias: bool) -> RbResult<Option<f64>> {
        let out = self
            .series
            .read()
            .kurtosis(fisher, bias)
            .map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn cast(&self, dtype: Wrap<DataType>, strict: bool) -> RbResult<Self> {
        let dtype = dtype.0;
        let out = if strict {
            self.series.read().strict_cast(&dtype)
        } else {
            self.series.read().cast(&dtype)
        };
        let out = out.map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn get_chunks(ruby: &Ruby, self_: &Self) -> RbResult<RArray> {
        ruby.ary_try_from_iter(
            flatten_series(&self_.series.read())
                .into_iter()
                .map(|s| rb_modules::pl_utils().funcall::<_, _, Value>("wrap_s", (Self::new(s),))),
        )
    }

    pub fn is_sorted(&self, descending: bool, nulls_last: bool) -> RbResult<bool> {
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded: true,
            maintain_order: false,
            limit: None,
        };
        Ok(self
            .series
            .read()
            .is_sorted(options)
            .map_err(RbPolarsErr::from)?)
    }

    pub fn clear(&self) -> Self {
        self.series.read().clear().into()
    }

    pub fn time_unit(&self) -> Option<String> {
        if let DataType::Datetime(tu, _) | DataType::Duration(tu) = self.series.read().dtype() {
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
            let binding = filter.series.read();
            let mask = binding.bool()?;
            let ca = series.$cast()?;
            let new = ca.set(mask, value)?;
            Ok(new.into_series())
        }

        impl RbSeries {
            pub fn $name(&self, filter: &RbSeries, value: Option<$native>) -> RbResult<Self> {
                let series =
                    $name(&self.series.read(), filter, value).map_err(RbPolarsErr::from)?;
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
            .read()
            .clone()
            .extend_constant(value.0, n)
            .map_err(RbPolarsErr::from)?
            .into())
    }
}

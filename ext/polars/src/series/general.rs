use magnus::{IntoValue, RArray, Ruby, Value, value::ReprValue};
use polars::prelude::*;
use polars::series::IsSorted;
use polars_core::chunked_array::cast::CastOptions;
use polars_core::utils::flatten::flatten_series;

use crate::conversion::*;
use crate::exceptions::{RbIndexError, RbRuntimeError, RbValueError};
use crate::prelude::*;
use crate::utils::{EnterPolarsExt, RubyAttach};
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

    pub fn reshape(rb: &Ruby, self_: &Self, dims: Vec<i64>) -> RbResult<Self> {
        let dims = dims
            .into_iter()
            .map(ReshapeDimension::new)
            .collect::<Vec<_>>();

        rb.enter_polars_series(|| self_.series.read().reshape_array(&dims))
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
                rb_modules::pl_utils(ruby).funcall("wrap_s", (rbseries,))
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

    pub fn bitand(rb: &Ruby, self_: &Self, other: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| &*self_.series.read() & &*other.series.read())
    }

    pub fn bitor(rb: &Ruby, self_: &Self, other: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| &*self_.series.read() | &*other.series.read())
    }

    pub fn bitxor(rb: &Ruby, self_: &Self, other: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| &*self_.series.read() ^ &*other.series.read())
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

    pub fn dtype(rb: &Ruby, self_: &Self) -> Value {
        Wrap(self_.series.read().dtype().clone()).into_value_with(rb)
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

    pub fn new_from_index(rb: &Ruby, self_: &Self, index: usize, length: usize) -> RbResult<Self> {
        let s = self_.series.read();
        if index >= s.len() {
            Err(RbValueError::new_err("index is out of bounds"))
        } else {
            rb.enter_polars_series(|| Ok(s.new_from_index(index, length)))
        }
    }

    pub fn filter(rb: &Ruby, self_: &Self, filter: &RbSeries) -> RbResult<Self> {
        let filter_series = &filter.series.read();
        if let Ok(ca) = filter_series.bool() {
            rb.enter_polars_series(|| self_.series.read().filter(ca))
        } else {
            Err(RbRuntimeError::new_err("Expected a boolean mask"))
        }
    }

    pub fn sort(
        rb: &Ruby,
        self_: &Self,
        descending: bool,
        nulls_last: bool,
        multithreaded: bool,
    ) -> RbResult<Self> {
        rb.enter_polars_series(|| {
            self_.series.read().sort(
                SortOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last)
                    .with_multithreaded(multithreaded),
            )
        })
    }

    pub fn gather_with_series(rb: &Ruby, self_: &Self, indices: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().take(indices.series.read().idx()?))
    }

    pub fn null_count(&self) -> RbResult<usize> {
        Ok(self.series.read().null_count())
    }

    pub fn has_nulls(&self) -> bool {
        self.series.read().has_nulls()
    }

    pub fn equals(
        rb: &Ruby,
        self_: &Self,
        other: &RbSeries,
        check_dtypes: bool,
        check_names: bool,
        null_equal: bool,
    ) -> RbResult<bool> {
        let s = self_.series.read();
        let o = other.series.read();
        if check_dtypes && (s.dtype() != o.dtype()) {
            return Ok(false);
        }
        if check_names && (s.name() != o.name()) {
            return Ok(false);
        }
        if null_equal {
            rb.enter_polars_ok(|| s.equals_missing(&o))
        } else {
            rb.enter_polars_ok(|| s.equals(&o))
        }
    }

    pub fn as_str(&self) -> String {
        format!("{:?}", self.series.read())
    }

    pub fn len(&self) -> usize {
        self.series.read().len()
    }

    pub fn clone(&self) -> Self {
        RbSeries::new(self.series.read().clone())
    }

    pub fn zip_with(rb: &Ruby, self_: &Self, mask: &RbSeries, other: &RbSeries) -> RbResult<Self> {
        let ms = mask.series.read();
        let mask = ms.bool().map_err(RbPolarsErr::from)?;
        rb.enter_polars_series(|| self_.series.read().zip_with(mask, &other.series.read()))
    }

    pub fn to_dummies(
        rb: &Ruby,
        self_: &Self,
        separator: Option<String>,
        drop_first: bool,
        drop_nulls: bool,
    ) -> RbResult<RbDataFrame> {
        rb.enter_polars_df(|| {
            self_
                .series
                .read()
                .to_dummies(separator.as_deref(), drop_first, drop_nulls)
        })
    }

    pub fn n_unique(rb: &Ruby, self_: &Self) -> RbResult<usize> {
        rb.enter_polars(|| self_.series.read().n_unique())
    }

    pub fn floor(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars_series(|| self_.series.read().floor())
    }

    pub fn shrink_to_fit(rb: &Ruby, self_: &Self) -> RbResult<()> {
        rb.enter_polars_ok(|| self_.series.write().shrink_to_fit())
    }

    pub fn dot(&self, other: &RbSeries) -> RbResult<f64> {
        let out = self
            .series
            .read()
            .dot(&other.series.read())
            .map_err(RbPolarsErr::from)?;
        Ok(out)
    }

    pub fn skew(rb: &Ruby, self_: &Self, bias: bool) -> RbResult<Option<f64>> {
        rb.enter_polars(|| self_.series.read().skew(bias))
    }

    pub fn kurtosis(rb: &Ruby, self_: &Self, fisher: bool, bias: bool) -> RbResult<Option<f64>> {
        rb.enter_polars(|| self_.series.read().kurtosis(fisher, bias))
    }

    pub fn cast(
        rb: &Ruby,
        self_: &Self,
        dtype: Wrap<DataType>,
        strict: bool,
        wrap_numerical: bool,
    ) -> RbResult<Self> {
        let options = if wrap_numerical {
            CastOptions::Overflowing
        } else if strict {
            CastOptions::Strict
        } else {
            CastOptions::NonStrict
        };
        rb.enter_polars_series(|| self_.series.read().cast_with_options(&dtype.0, options))
    }

    pub fn get_chunks(&self) -> RbResult<RArray> {
        Ruby::attach(|rb| {
            rb.ary_try_from_iter(flatten_series(&self.series.read()).into_iter().map(|s| {
                rb_modules::pl_utils(rb).funcall::<_, _, Value>("wrap_s", (Self::new(s),))
            }))
        })
    }

    pub fn is_sorted(
        rb: &Ruby,
        self_: &Self,
        descending: bool,
        nulls_last: bool,
    ) -> RbResult<bool> {
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded: true,
            maintain_order: false,
            limit: None,
        };
        rb.enter_polars(|| self_.series.read().is_sorted(options))
    }

    pub fn clear(&self) -> Self {
        self.series.read().clear().into()
    }

    pub fn head(rb: &Ruby, self_: &Self, n: usize) -> RbResult<Self> {
        rb.enter_polars_series(|| Ok(self_.series.read().head(Some(n))))
    }

    pub fn tail(rb: &Ruby, self_: &Self, n: usize) -> RbResult<Self> {
        rb.enter_polars_series(|| Ok(self_.series.read().tail(Some(n))))
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

    pub fn not_(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars_series(|| polars_ops::series::negate_bitwise(&self_.series.read()))
    }

    pub fn shrink_dtype(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars(|| {
            self_
                .series
                .read()
                .shrink_type()
                .map(Into::into)
                .map_err(RbPolarsErr::from)
                .map_err(RbErr::from)
        })
    }

    pub fn str_to_decimal_infer(
        rb: &Ruby,
        self_: &Self,
        inference_length: usize,
    ) -> RbResult<Self> {
        rb.enter_polars_series(|| {
            let s = self_.series.read();
            let ca = s.str()?;
            ca.to_decimal_infer(inference_length)
        })
    }

    pub fn str_json_decode(
        rb: &Ruby,
        self_: &Self,
        infer_schema_length: Option<usize>,
    ) -> RbResult<Self> {
        rb.enter_polars(|| {
            let lock = self_.series.read();
            lock.str()?
                .json_decode(None, infer_schema_length)
                .map(|s| s.with_name(lock.name().clone()))
        })
        .map(Into::into)
        .map_err(RbPolarsErr::from)
        .map_err(RbErr::from)
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

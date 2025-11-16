use std::hash::BuildHasher;

use either::Either;
use magnus::{IntoValue, RArray, Ruby, Value, prelude::*};
use polars::prelude::pivot::{pivot, pivot_stable};
use polars::prelude::*;

use crate::conversion::*;
use crate::exceptions::RbIndexError;
use crate::map::dataframe::{
    apply_lambda_unknown, apply_lambda_with_bool_out_type, apply_lambda_with_primitive_out_type,
    apply_lambda_with_utf8_out_type,
};
use crate::prelude::strings_to_pl_smallstr;
use crate::series::{to_rbseries, to_series};
use crate::utils::EnterPolarsExt;
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbPolarsErr, RbResult, RbSeries};

impl RbDataFrame {
    pub fn init(columns: RArray) -> RbResult<Self> {
        let mut cols = Vec::new();
        for i in columns.into_iter() {
            cols.push(<&RbSeries>::try_convert(i)?.series.read().clone().into());
        }
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn estimated_size(&self) -> usize {
        self.df.read().estimated_size()
    }

    pub fn dtype_strings(&self) -> Vec<String> {
        self.df
            .read()
            .get_columns()
            .iter()
            .map(|s| format!("{}", s.dtype()))
            .collect()
    }

    pub fn add(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() + &*s.series.read())
    }

    pub fn sub(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() - &*s.series.read())
    }

    pub fn div(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() / &*s.series.read())
    }

    pub fn mul(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() * &*s.series.read())
    }

    pub fn rem(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() % &*s.series.read())
    }

    pub fn add_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() + &*s.df.read())
    }

    pub fn sub_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() - &*s.df.read())
    }

    pub fn div_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() / &*s.df.read())
    }

    pub fn mul_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() * &*s.df.read())
    }

    pub fn rem_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.read() % &*s.df.read())
    }

    pub fn sample_n(
        rb: &Ruby,
        self_: &Self,
        n: &RbSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            self_
                .df
                .read()
                .sample_n(&n.series.read(), with_replacement, shuffle, seed)
        })
    }

    pub fn sample_frac(
        rb: &Ruby,
        self_: &Self,
        frac: &RbSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            self_
                .df
                .read()
                .sample_frac(&frac.series.read(), with_replacement, shuffle, seed)
        })
    }

    pub fn rechunk(rb: &Ruby, self_: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            let mut df = self_.df.write().clone();
            df.as_single_chunk_par();
            Ok(df)
        })
    }

    pub fn as_str(&self) -> String {
        format!("{}", self.df.read())
    }

    pub fn get_columns(&self) -> RArray {
        let cols = self.df.read().get_columns().to_vec();
        to_rbseries(cols)
    }

    pub fn columns(&self) -> Vec<String> {
        self.df
            .read()
            .get_column_names()
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    pub fn set_column_names(&self, names: Vec<String>) -> RbResult<()> {
        self.df
            .write()
            .set_column_names(&names)
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn dtypes(ruby: &Ruby, self_: &Self) -> RArray {
        ruby.ary_from_iter(
            self_
                .df
                .read()
                .iter()
                .map(|s| Wrap(s.dtype().clone()).into_value_with(ruby)),
        )
    }

    pub fn n_chunks(&self) -> usize {
        self.df.read().first_col_n_chunks()
    }

    pub fn shape(&self) -> (usize, usize) {
        self.df.read().shape()
    }

    pub fn height(&self) -> usize {
        self.df.read().height()
    }

    pub fn width(&self) -> usize {
        self.df.read().width()
    }

    pub fn hstack(rb: &Ruby, self_: &Self, columns: RArray) -> RbResult<Self> {
        let columns = to_series(columns)?;
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        rb.enter_polars_df(|| self_.df.read().hstack(&columns))
    }

    pub fn hstack_mut(rb: &Ruby, self_: &Self, columns: RArray) -> RbResult<()> {
        let columns = to_series(columns)?;
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        rb.enter_polars(|| self_.df.write().hstack_mut(&columns).map(drop))?;
        Ok(())
    }

    pub fn vstack(rb: &Ruby, self_: &Self, other: &RbDataFrame) -> RbResult<Self> {
        rb.enter_polars_df(|| self_.df.read().vstack(&other.df.read()))
    }

    pub fn vstack_mut(rb: &Ruby, self_: &Self, other: &RbDataFrame) -> RbResult<()> {
        rb.enter_polars(|| {
            // Prevent self-vstack deadlocks.
            let other = other.df.read().clone();
            self_.df.write().vstack_mut(&other)?;
            PolarsResult::Ok(())
        })?;
        Ok(())
    }

    pub fn extend(rb: &Ruby, self_: &Self, other: &RbDataFrame) -> RbResult<()> {
        rb.enter_polars(|| {
            // Prevent self-extend deadlocks.
            let other = other.df.read().clone();
            self_.df.write().extend(&other)
        })?;
        Ok(())
    }

    pub fn drop_in_place(&self, name: String) -> RbResult<RbSeries> {
        let s = self
            .df
            .write()
            .drop_in_place(&name)
            .map_err(RbPolarsErr::from)?;
        let s = s.take_materialized_series();
        Ok(RbSeries::new(s))
    }

    pub fn to_series(&self, index: isize) -> RbResult<RbSeries> {
        let df = &self.df.read();

        let index_adjusted = if index < 0 {
            df.width().checked_sub(index.unsigned_abs())
        } else {
            Some(usize::try_from(index).unwrap())
        };

        let s = index_adjusted.and_then(|i| df.select_at_idx(i));
        match s {
            Some(s) => Ok(RbSeries::new(s.as_materialized_series().clone())),
            None => Err(RbIndexError::new_err(
                polars_err!(oob = index, df.width()).to_string(),
            )),
        }
    }

    pub fn get_column_index(&self, name: String) -> Option<usize> {
        self.df.read().get_column_index(&name)
    }

    pub fn get_column(&self, name: String) -> RbResult<RbSeries> {
        let series = self
            .df
            .read()
            .column(&name)
            .map(|s| RbSeries::new(s.as_materialized_series().clone()))
            .map_err(RbPolarsErr::from)?;
        Ok(series)
    }

    pub fn select(rb: &Ruby, self_: &Self, columns: Vec<String>) -> RbResult<Self> {
        rb.enter_polars_df(|| self_.df.read().select(columns.iter().map(|x| &**x)))
    }

    pub fn gather(rb: &Ruby, self_: &Self, indices: Vec<IdxSize>) -> RbResult<Self> {
        let indices = IdxCa::from_vec("".into(), indices);
        rb.enter_polars_df(|| self_.df.read().take(&indices))
    }

    pub fn gather_with_series(rb: &Ruby, self_: &Self, indices: &RbSeries) -> RbResult<Self> {
        let idx_s = indices.series.read();
        let indices = idx_s.idx().map_err(RbPolarsErr::from)?;
        rb.enter_polars_df(|| self_.df.read().take(indices))
    }

    pub fn replace(&self, column: String, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .write()
            .replace(&column, new_col.series.read().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn replace_column(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .write()
            .replace_column(index, new_col.series.read().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn insert_column(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .write()
            .insert_column(index, new_col.series.read().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn slice(rb: &Ruby, self_: &Self, offset: i64, length: Option<usize>) -> RbResult<Self> {
        rb.enter_polars_df(|| {
            let df = self_.df.read();
            Ok(df.slice(offset, length.unwrap_or_else(|| df.height())))
        })
    }

    pub fn head(rb: &Ruby, self_: &Self, n: usize) -> RbResult<Self> {
        rb.enter_polars_df(|| Ok(self_.df.read().head(Some(n))))
    }

    pub fn tail(rb: &Ruby, self_: &Self, n: usize) -> RbResult<Self> {
        rb.enter_polars_df(|| Ok(self_.df.read().tail(Some(n))))
    }

    pub fn is_unique(rb: &Ruby, self_: &Self) -> RbResult<RbSeries> {
        rb.enter_polars_series(|| self_.df.read().is_unique())
    }

    pub fn is_duplicated(rb: &Ruby, self_: &Self) -> RbResult<RbSeries> {
        rb.enter_polars_series(|| self_.df.read().is_duplicated())
    }

    pub fn equals(
        rb: &Ruby,
        self_: &Self,
        other: &RbDataFrame,
        null_equal: bool,
    ) -> RbResult<bool> {
        if null_equal {
            rb.enter_polars_ok(|| self_.df.read().equals_missing(&other.df.read()))
        } else {
            rb.enter_polars_ok(|| self_.df.read().equals(&other.df.read()))
        }
    }

    pub fn with_row_index(
        rb: &Ruby,
        self_: &Self,
        name: String,
        offset: Option<IdxSize>,
    ) -> RbResult<Self> {
        rb.enter_polars_df(|| self_.df.read().with_row_index(name.into(), offset))
    }

    pub fn clone(&self) -> Self {
        RbDataFrame::new(self.df.read().clone())
    }

    pub fn unpivot(
        rb: &Ruby,
        self_: &Self,
        on: Vec<String>,
        index: Vec<String>,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> RbResult<Self> {
        let args = UnpivotArgsIR {
            on: strings_to_pl_smallstr(on),
            index: strings_to_pl_smallstr(index),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
        };

        rb.enter_polars_df(|| self_.df.read().unpivot2(args))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pivot_expr(
        rb: &Ruby,
        self_: &Self,
        on: Vec<String>,
        index: Option<Vec<String>>,
        values: Option<Vec<String>>,
        maintain_order: bool,
        sort_columns: bool,
        aggregate_expr: Option<&RbExpr>,
        separator: Option<String>,
    ) -> RbResult<Self> {
        let df = self_.df.read().clone(); // Clone to avoid dead lock on re-entrance in aggregate_expr.
        let fun = if maintain_order { pivot_stable } else { pivot };
        let agg_expr = aggregate_expr.map(|expr| expr.inner.clone());
        rb.enter_polars_df(|| {
            fun(
                &df,
                on,
                index,
                values,
                sort_columns,
                agg_expr,
                separator.as_deref(),
            )
        })
    }

    pub fn partition_by(
        ruby: &Ruby,
        self_: &Self,
        by: Vec<String>,
        maintain_order: bool,
        include_key: bool,
    ) -> RbResult<RArray> {
        let out = if maintain_order {
            self_.df.read().partition_by_stable(by, include_key)
        } else {
            self_.df.read().partition_by(by, include_key)
        }
        .map_err(RbPolarsErr::from)?;
        Ok(ruby.ary_from_iter(out.into_iter().map(RbDataFrame::new)))
    }

    pub fn lazy(&self) -> RbLazyFrame {
        self.df.read().clone().lazy().into()
    }

    pub fn to_dummies(
        &self,
        columns: Option<Vec<String>>,
        separator: Option<String>,
        drop_first: bool,
        drop_nulls: bool,
    ) -> RbResult<Self> {
        let df = match columns {
            Some(cols) => self.df.read().columns_to_dummies(
                cols.iter().map(|x| x as &str).collect(),
                separator.as_deref(),
                drop_first,
                drop_nulls,
            ),
            None => self
                .df
                .read()
                .to_dummies(separator.as_deref(), drop_first, drop_nulls),
        }
        .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn null_count(&self) -> Self {
        let df = self.df.read().null_count();
        df.into()
    }

    pub fn map_rows(
        ruby: &Ruby,
        self_: &Self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        inference_size: usize,
    ) -> RbResult<(Value, bool)> {
        let df = &self_.df.read();

        let output_type = output_type.map(|dt| dt.0);
        let out = match output_type {
            Some(DataType::Int32) => {
                apply_lambda_with_primitive_out_type::<Int32Type>(df, lambda, 0, None).into_series()
            }
            Some(DataType::Int64) => {
                apply_lambda_with_primitive_out_type::<Int64Type>(df, lambda, 0, None).into_series()
            }
            Some(DataType::UInt32) => {
                apply_lambda_with_primitive_out_type::<UInt32Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::UInt64) => {
                apply_lambda_with_primitive_out_type::<UInt64Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::Float32) => {
                apply_lambda_with_primitive_out_type::<Float32Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::Float64) => {
                apply_lambda_with_primitive_out_type::<Float64Type>(df, lambda, 0, None)
                    .into_series()
            }
            Some(DataType::Boolean) => {
                apply_lambda_with_bool_out_type(df, lambda, 0, None).into_series()
            }
            Some(DataType::Date) => {
                apply_lambda_with_primitive_out_type::<Int32Type>(df, lambda, 0, None)
                    .into_date()
                    .into_series()
            }
            Some(DataType::Datetime(tu, tz)) => {
                apply_lambda_with_primitive_out_type::<Int64Type>(df, lambda, 0, None)
                    .into_datetime(tu, tz)
                    .into_series()
            }
            Some(DataType::String) => {
                apply_lambda_with_utf8_out_type(df, lambda, 0, None).into_series()
            }
            _ => return apply_lambda_unknown(df, lambda, inference_size),
        };

        Ok((ruby.obj_wrap(RbSeries::from(out)).as_value(), false))
    }

    pub fn shrink_to_fit(&self) {
        self.df.write().shrink_to_fit();
    }

    pub fn hash_rows(&self, k0: u64, k1: u64, k2: u64, k3: u64) -> RbResult<RbSeries> {
        let seed = PlFixedStateQuality::default().hash_one((k0, k1, k2, k3));
        let hb = PlSeedableRandomStateQuality::seed_from_u64(seed);
        let hash = self
            .df
            .write()
            .hash_rows(Some(hb))
            .map_err(RbPolarsErr::from)?;
        Ok(hash.into_series().into())
    }

    pub fn transpose(&self, keep_names_as: Option<String>, column_names: Value) -> RbResult<Self> {
        let new_col_names = if let Ok(name) = <Vec<String>>::try_convert(column_names) {
            Some(Either::Right(name))
        } else if let Ok(name) = String::try_convert(column_names) {
            Some(Either::Left(name))
        } else {
            None
        };
        Ok(self
            .df
            .write()
            .transpose(keep_names_as.as_deref(), new_col_names)
            .map_err(RbPolarsErr::from)?
            .into())
    }

    pub fn upsample(
        &self,
        by: Vec<String>,
        index_column: String,
        every: String,
        stable: bool,
    ) -> RbResult<Self> {
        let out = if stable {
            self.df
                .read()
                .upsample_stable(by, &index_column, Duration::parse(&every))
        } else {
            self.df
                .read()
                .upsample(by, &index_column, Duration::parse(&every))
        };
        let out = out.map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn to_struct(&self, name: String) -> RbSeries {
        let s = self.df.read().clone().into_struct(name.into());
        s.into_series().into()
    }

    pub fn clear(&self) -> Self {
        self.df.read().clear().into()
    }
}

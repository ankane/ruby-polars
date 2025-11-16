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
            cols.push(<&RbSeries>::try_convert(i)?.series.borrow().clone().into());
        }
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn estimated_size(&self) -> usize {
        self.df.borrow().estimated_size()
    }

    pub fn dtype_strings(&self) -> Vec<String> {
        self.df
            .borrow()
            .get_columns()
            .iter()
            .map(|s| format!("{}", s.dtype()))
            .collect()
    }

    pub fn add(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() + &*s.series.borrow())
    }

    pub fn sub(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() - &*s.series.borrow())
    }

    pub fn div(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() / &*s.series.borrow())
    }

    pub fn mul(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() * &*s.series.borrow())
    }

    pub fn rem(rb: &Ruby, self_: &Self, s: &RbSeries) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() % &*s.series.borrow())
    }

    pub fn add_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() + &*s.df.borrow())
    }

    pub fn sub_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() - &*s.df.borrow())
    }

    pub fn div_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() / &*s.df.borrow())
    }

    pub fn mul_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() * &*s.df.borrow())
    }

    pub fn rem_df(rb: &Ruby, self_: &Self, s: &Self) -> RbResult<Self> {
        rb.enter_polars_df(|| &*self_.df.borrow() % &*s.df.borrow())
    }

    pub fn sample_n(
        &self,
        n: &RbSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .sample_n(&n.series.borrow(), with_replacement, shuffle, seed)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn sample_frac(
        &self,
        frac: &RbSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<u64>,
    ) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .sample_frac(&frac.series.borrow(), with_replacement, shuffle, seed)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn rechunk(&self) -> Self {
        let mut df = self.df.borrow_mut().clone();
        df.as_single_chunk_par();
        df.into()
    }

    pub fn as_str(&self) -> String {
        format!("{}", self.df.borrow())
    }

    pub fn get_columns(&self) -> RArray {
        let cols = self.df.borrow().get_columns().to_vec();
        to_rbseries(cols)
    }

    pub fn columns(&self) -> Vec<String> {
        self.df
            .borrow()
            .get_column_names()
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    pub fn set_column_names(&self, names: Vec<String>) -> RbResult<()> {
        self.df
            .borrow_mut()
            .set_column_names(&names)
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn dtypes(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .df
                .borrow()
                .iter()
                .map(|s| Wrap(s.dtype().clone()).into_value_with(ruby)),
        )
    }

    pub fn n_chunks(&self) -> usize {
        self.df.borrow().first_col_n_chunks()
    }

    pub fn shape(&self) -> (usize, usize) {
        self.df.borrow().shape()
    }

    pub fn height(&self) -> usize {
        self.df.borrow().height()
    }

    pub fn width(&self) -> usize {
        self.df.borrow().width()
    }

    pub fn hstack(&self, columns: RArray) -> RbResult<Self> {
        let columns = to_series(columns)?;
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        let df = self
            .df
            .borrow()
            .hstack(&columns)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn hstack_mut(&self, columns: RArray) -> RbResult<()> {
        let columns = to_series(columns)?;
        let columns = columns.into_iter().map(Into::into).collect::<Vec<_>>();
        self.df
            .borrow_mut()
            .hstack_mut(&columns)
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn vstack(&self, df: &RbDataFrame) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .vstack(&df.df.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn vstack_mut(&self, df: &RbDataFrame) -> RbResult<()> {
        self.df
            .borrow_mut()
            .vstack_mut(&df.df.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn extend(&self, df: &RbDataFrame) -> RbResult<()> {
        self.df
            .borrow_mut()
            .extend(&df.df.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn drop_in_place(&self, name: String) -> RbResult<RbSeries> {
        let s = self
            .df
            .borrow_mut()
            .drop_in_place(&name)
            .map_err(RbPolarsErr::from)?;
        let s = s.take_materialized_series();
        Ok(RbSeries::new(s))
    }

    pub fn to_series(&self, index: isize) -> RbResult<RbSeries> {
        let df = &self.df.borrow();

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
        self.df.borrow().get_column_index(&name)
    }

    pub fn get_column(&self, name: String) -> RbResult<RbSeries> {
        let series = self
            .df
            .borrow()
            .column(&name)
            .map(|s| RbSeries::new(s.as_materialized_series().clone()))
            .map_err(RbPolarsErr::from)?;
        Ok(series)
    }

    pub fn select(&self, selection: Vec<String>) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .select(selection)
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn gather(&self, indices: Vec<IdxSize>) -> RbResult<Self> {
        let indices = IdxCa::from_vec("".into(), indices);
        let df = self.df.borrow().take(&indices).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn take_with_series(&self, indices: &RbSeries) -> RbResult<Self> {
        let binding = indices.series.borrow();
        let idx = binding.idx().map_err(RbPolarsErr::from)?;
        let df = self.df.borrow().take(idx).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn replace(&self, column: String, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .borrow_mut()
            .replace(&column, new_col.series.borrow().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn replace_column(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .borrow_mut()
            .replace_column(index, new_col.series.borrow().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn insert_column(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .borrow_mut()
            .insert_column(index, new_col.series.borrow().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn slice(&self, offset: i64, length: Option<usize>) -> Self {
        let df = self
            .df
            .borrow()
            .slice(offset, length.unwrap_or_else(|| self.df.borrow().height()));
        df.into()
    }

    pub fn head(&self, length: Option<usize>) -> Self {
        self.df.borrow().head(length).into()
    }

    pub fn tail(&self, length: Option<usize>) -> Self {
        self.df.borrow().tail(length).into()
    }

    pub fn is_unique(rb: &Ruby, self_: &Self) -> RbResult<RbSeries> {
        rb.enter_polars_series(|| self_.df.borrow().is_unique())
    }

    pub fn is_duplicated(&self) -> RbResult<RbSeries> {
        let mask = self
            .df
            .borrow()
            .is_duplicated()
            .map_err(RbPolarsErr::from)?;
        Ok(mask.into_series().into())
    }

    pub fn equals(&self, other: &RbDataFrame, null_equal: bool) -> bool {
        if null_equal {
            self.df.borrow().equals_missing(&other.df.borrow())
        } else {
            self.df.borrow().equals(&other.df.borrow())
        }
    }

    pub fn with_row_index(&self, name: String, offset: Option<IdxSize>) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .with_row_index(name.into(), offset)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn clone(&self) -> Self {
        RbDataFrame::new(self.df.borrow().clone())
    }

    pub fn unpivot(
        &self,
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

        let df = self.df.borrow().unpivot2(args).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pivot_expr(
        &self,
        on: Vec<String>,
        index: Option<Vec<String>>,
        values: Option<Vec<String>>,
        maintain_order: bool,
        sort_columns: bool,
        aggregate_expr: Option<&RbExpr>,
        separator: Option<String>,
    ) -> RbResult<Self> {
        let fun = if maintain_order { pivot_stable } else { pivot };
        let agg_expr = aggregate_expr.map(|aggregate_expr| aggregate_expr.inner.clone());
        let df = fun(
            &self.df.borrow(),
            on,
            index,
            values,
            sort_columns,
            agg_expr,
            separator.as_deref(),
        )
        .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn partition_by(
        ruby: &Ruby,
        rb_self: &Self,
        by: Vec<String>,
        maintain_order: bool,
        include_key: bool,
    ) -> RbResult<RArray> {
        let out = if maintain_order {
            rb_self.df.borrow().partition_by_stable(by, include_key)
        } else {
            rb_self.df.borrow().partition_by(by, include_key)
        }
        .map_err(RbPolarsErr::from)?;
        Ok(ruby.ary_from_iter(out.into_iter().map(RbDataFrame::new)))
    }

    pub fn lazy(&self) -> RbLazyFrame {
        self.df.borrow().clone().lazy().into()
    }

    pub fn to_dummies(
        &self,
        columns: Option<Vec<String>>,
        separator: Option<String>,
        drop_first: bool,
        drop_nulls: bool,
    ) -> RbResult<Self> {
        let df = match columns {
            Some(cols) => self.df.borrow().columns_to_dummies(
                cols.iter().map(|x| x as &str).collect(),
                separator.as_deref(),
                drop_first,
                drop_nulls,
            ),
            None => self
                .df
                .borrow()
                .to_dummies(separator.as_deref(), drop_first, drop_nulls),
        }
        .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn null_count(&self) -> Self {
        let df = self.df.borrow().null_count();
        df.into()
    }

    pub fn map_rows(
        ruby: &Ruby,
        rb_self: &Self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        inference_size: usize,
    ) -> RbResult<(Value, bool)> {
        let df = &rb_self.df.borrow();

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
        self.df.borrow_mut().shrink_to_fit();
    }

    pub fn hash_rows(&self, k0: u64, k1: u64, k2: u64, k3: u64) -> RbResult<RbSeries> {
        let seed = PlFixedStateQuality::default().hash_one((k0, k1, k2, k3));
        let hb = PlSeedableRandomStateQuality::seed_from_u64(seed);
        let hash = self
            .df
            .borrow_mut()
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
            .borrow_mut()
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
                .borrow()
                .upsample_stable(by, &index_column, Duration::parse(&every))
        } else {
            self.df
                .borrow()
                .upsample(by, &index_column, Duration::parse(&every))
        };
        let out = out.map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn to_struct(&self, name: String) -> RbSeries {
        let s = self.df.borrow().clone().into_struct(name.into());
        s.into_series().into()
    }

    pub fn clear(&self) -> Self {
        self.df.borrow().clear().into()
    }
}

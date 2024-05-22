use either::Either;
use magnus::{prelude::*, r_hash::ForEach, typed_data::Obj, IntoValue, RArray, RHash, Value};
use polars::frame::row::{rows_to_schema_supertypes, Row};
use polars::frame::NullStrategy;
use polars::prelude::pivot::{pivot, pivot_stable};
use polars::prelude::*;

use crate::conversion::*;
use crate::map::dataframe::{
    apply_lambda_unknown, apply_lambda_with_bool_out_type, apply_lambda_with_primitive_out_type,
    apply_lambda_with_utf8_out_type,
};
use crate::rb_modules;
use crate::series::{to_rbseries_collection, to_series_collection};
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbPolarsErr, RbResult, RbSeries};

impl RbDataFrame {
    fn finish_from_rows(
        rows: Vec<Row>,
        infer_schema_length: Option<usize>,
        schema: Option<Schema>,
        schema_overrides_by_idx: Option<Vec<(usize, DataType)>>,
    ) -> RbResult<Self> {
        // Object builder must be registered
        crate::on_startup::register_object_builder();

        let mut final_schema =
            rows_to_schema_supertypes(&rows, infer_schema_length.map(|n| std::cmp::max(1, n)))
                .map_err(RbPolarsErr::from)?;

        // Erase scale from inferred decimals.
        for dtype in final_schema.iter_dtypes_mut() {
            if let DataType::Decimal(_, _) = dtype {
                *dtype = DataType::Decimal(None, None)
            }
        }

        // Integrate explicit/inferred schema.
        if let Some(schema) = schema {
            for (i, (name, dtype)) in schema.into_iter().enumerate() {
                if let Some((name_, dtype_)) = final_schema.get_at_index_mut(i) {
                    *name_ = name;

                    // If schema dtype is Unknown, overwrite with inferred datatype.
                    if !matches!(dtype, DataType::Unknown(_)) {
                        *dtype_ = dtype;
                    }
                } else {
                    final_schema.with_column(name, dtype);
                }
            }
        }

        // Optional per-field overrides; these supersede default/inferred dtypes.
        if let Some(overrides) = schema_overrides_by_idx {
            for (i, dtype) in overrides {
                if let Some((_, dtype_)) = final_schema.get_at_index_mut(i) {
                    if !matches!(dtype, DataType::Unknown(_)) {
                        *dtype_ = dtype;
                    }
                }
            }
        }
        let df =
            DataFrame::from_rows_and_schema(&rows, &final_schema).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn init(columns: RArray) -> RbResult<Self> {
        let mut cols = Vec::new();
        for i in columns.each() {
            cols.push(<&RbSeries>::try_convert(i?)?.series.borrow().clone());
        }
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn estimated_size(&self) -> usize {
        self.df.borrow().estimated_size()
    }

    pub fn read_rows(
        rb_rows: RArray,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let mut rows = Vec::with_capacity(rb_rows.len());
        for v in rb_rows.each() {
            let rb_row = RArray::try_convert(v?)?;
            let mut row = Vec::with_capacity(rb_row.len());
            for val in rb_row.each() {
                row.push(Wrap::<AnyValue>::try_convert(val?)?.0);
            }
            rows.push(Row(row));
        }
        Self::finish_from_rows(rows, infer_schema_length, schema.map(|wrap| wrap.0), None)
    }

    pub fn read_hashes(
        dicts: Value,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
        schema_overrides: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let mut schema_columns = PlIndexSet::new();
        if let Some(s) = &schema {
            schema_columns.extend(s.0.iter_names().map(|n| n.to_string()))
        }
        let (rows, names) = dicts_to_rows(&dicts, infer_schema_length, schema_columns)?;

        let mut schema_overrides_by_idx: Vec<(usize, DataType)> = Vec::new();
        if let Some(overrides) = schema_overrides {
            for (idx, name) in names.iter().enumerate() {
                if let Some(dtype) = overrides.0.get(name) {
                    schema_overrides_by_idx.push((idx, dtype.clone()));
                }
            }
        }
        let rbdf = Self::finish_from_rows(
            rows,
            infer_schema_length,
            schema.map(|wrap| wrap.0),
            Some(schema_overrides_by_idx),
        )?;

        unsafe {
            rbdf.df
                .borrow_mut()
                .get_columns_mut()
                .iter_mut()
                .zip(&names)
                .for_each(|(s, name)| {
                    s.rename(name);
                });
        }
        let length = names.len();
        if names.into_iter().collect::<PlHashSet<_>>().len() != length {
            let err = PolarsError::SchemaMismatch("duplicate column names found".into());
            Err(RbPolarsErr::from(err))?;
        }

        Ok(rbdf)
    }

    pub fn read_hash(data: RHash) -> RbResult<Self> {
        let mut cols: Vec<Series> = Vec::new();
        data.foreach(|name: String, values: Value| {
            let obj: Value = rb_modules::series().funcall("new", (name, values))?;
            let rbseries = obj.funcall::<_, _, &RbSeries>("_s", ())?;
            cols.push(rbseries.series.borrow().clone());
            Ok(ForEach::Continue)
        })?;
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn add(&self, s: &RbSeries) -> RbResult<Self> {
        let df = (&*self.df.borrow() + &*s.series.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn sub(&self, s: &RbSeries) -> RbResult<Self> {
        let df = (&*self.df.borrow() - &*s.series.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn div(&self, s: &RbSeries) -> RbResult<Self> {
        let df = (&*self.df.borrow() / &*s.series.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn mul(&self, s: &RbSeries) -> RbResult<Self> {
        let df = (&*self.df.borrow() * &*s.series.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn rem(&self, s: &RbSeries) -> RbResult<Self> {
        let df = (&*self.df.borrow() % &*s.series.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn add_df(&self, s: &Self) -> RbResult<Self> {
        let df = (&*self.df.borrow() + &*s.df.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn sub_df(&self, s: &Self) -> RbResult<Self> {
        let df = (&*self.df.borrow() - &*s.df.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn div_df(&self, s: &Self) -> RbResult<Self> {
        let df = (&*self.df.borrow() / &*s.df.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn mul_df(&self, s: &Self) -> RbResult<Self> {
        let df = (&*self.df.borrow() * &*s.df.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn rem_df(&self, s: &Self) -> RbResult<Self> {
        let df = (&*self.df.borrow() % &*s.df.borrow()).map_err(RbPolarsErr::from)?;
        Ok(df.into())
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

    pub fn to_s(&self) -> String {
        format!("{}", self.df.borrow())
    }

    pub fn get_columns(&self) -> RArray {
        let cols = self.df.borrow().get_columns().to_vec();
        to_rbseries_collection(cols)
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

    pub fn dtypes(&self) -> RArray {
        RArray::from_iter(
            self.df
                .borrow()
                .iter()
                .map(|s| Wrap(s.dtype().clone()).into_value()),
        )
    }

    pub fn n_chunks(&self) -> usize {
        self.df.borrow().n_chunks()
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

    pub fn hstack_mut(&self, columns: RArray) -> RbResult<()> {
        let columns = to_series_collection(columns)?;
        self.df
            .borrow_mut()
            .hstack_mut(&columns)
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn hstack(&self, columns: RArray) -> RbResult<Self> {
        let columns = to_series_collection(columns)?;
        let df = self
            .df
            .borrow()
            .hstack(&columns)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn extend(&self, df: &RbDataFrame) -> RbResult<()> {
        self.df
            .borrow_mut()
            .extend(&df.df.borrow())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn vstack_mut(&self, df: &RbDataFrame) -> RbResult<()> {
        self.df
            .borrow_mut()
            .vstack_mut(&df.df.borrow())
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

    pub fn drop_in_place(&self, name: String) -> RbResult<RbSeries> {
        let s = self
            .df
            .borrow_mut()
            .drop_in_place(&name)
            .map_err(RbPolarsErr::from)?;
        Ok(RbSeries::new(s))
    }

    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .drop_nulls(subset.as_ref().map(|s| s.as_ref()))
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn drop(&self, name: String) -> RbResult<Self> {
        let df = self.df.borrow().drop(&name).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn select_at_idx(&self, idx: usize) -> Option<RbSeries> {
        self.df
            .borrow()
            .select_at_idx(idx)
            .map(|s| RbSeries::new(s.clone()))
    }

    pub fn get_column_index(&self, name: String) -> Option<usize> {
        self.df.borrow().get_column_index(&name)
    }

    pub fn get_column(&self, name: String) -> RbResult<RbSeries> {
        self.df
            .borrow()
            .column(&name)
            .map(|s| RbSeries::new(s.clone()))
            .map_err(RbPolarsErr::from)
    }

    pub fn select(&self, selection: Vec<String>) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .select(selection)
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn take(&self, indices: Vec<IdxSize>) -> RbResult<Self> {
        let indices = IdxCa::from_vec("", indices);
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

    pub fn slice(&self, offset: usize, length: Option<usize>) -> Self {
        let df = self.df.borrow().slice(
            offset as i64,
            length.unwrap_or_else(|| self.df.borrow().height()),
        );
        df.into()
    }

    pub fn head(&self, length: Option<usize>) -> Self {
        self.df.borrow().head(length).into()
    }

    pub fn tail(&self, length: Option<usize>) -> Self {
        self.df.borrow().tail(length).into()
    }

    pub fn is_unique(&self) -> RbResult<RbSeries> {
        let mask = self.df.borrow().is_unique().map_err(RbPolarsErr::from)?;
        Ok(mask.into_series().into())
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
            .with_row_index(&name, offset)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn clone(&self) -> Self {
        RbDataFrame::new(self.df.borrow().clone())
    }

    pub fn melt(
        &self,
        id_vars: Vec<String>,
        value_vars: Vec<String>,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> RbResult<Self> {
        let args = MeltArgs {
            id_vars: strings_to_smartstrings(id_vars),
            value_vars: strings_to_smartstrings(value_vars),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
            streamable: false,
        };

        let df = self.df.borrow().melt2(args).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn pivot_expr(
        &self,
        index: Vec<String>,
        columns: Vec<String>,
        values: Option<Vec<String>>,
        maintain_order: bool,
        sort_columns: bool,
        aggregate_expr: Option<&RbExpr>,
        separator: Option<String>,
    ) -> RbResult<Self> {
        let fun = match maintain_order {
            true => pivot_stable,
            false => pivot,
        };
        let agg_expr = aggregate_expr.map(|aggregate_expr| aggregate_expr.inner.clone());
        let df = fun(
            &self.df.borrow(),
            index,
            columns,
            values,
            sort_columns,
            agg_expr,
            separator.as_deref(),
        )
        .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn partition_by(
        &self,
        by: Vec<String>,
        maintain_order: bool,
        include_key: bool,
    ) -> RbResult<RArray> {
        let out = if maintain_order {
            self.df.borrow().partition_by_stable(by, include_key)
        } else {
            self.df.borrow().partition_by(by, include_key)
        }
        .map_err(RbPolarsErr::from)?;
        Ok(RArray::from_iter(out.into_iter().map(RbDataFrame::new)))
    }

    pub fn shift(&self, periods: i64) -> Self {
        self.df.borrow().shift(periods).into()
    }

    pub fn lazy(&self) -> RbLazyFrame {
        self.df.borrow().clone().lazy().into()
    }

    pub fn max_horizontal(&self) -> RbResult<Option<RbSeries>> {
        let s = self
            .df
            .borrow()
            .max_horizontal()
            .map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn min_horizontal(&self) -> RbResult<Option<RbSeries>> {
        let s = self
            .df
            .borrow()
            .min_horizontal()
            .map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn sum_horizontal(&self, ignore_nulls: bool) -> RbResult<Option<RbSeries>> {
        let null_strategy = if ignore_nulls {
            NullStrategy::Ignore
        } else {
            NullStrategy::Propagate
        };
        let s = self
            .df
            .borrow()
            .sum_horizontal(null_strategy)
            .map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn mean_horizontal(&self, ignore_nulls: bool) -> RbResult<Option<RbSeries>> {
        let null_strategy = if ignore_nulls {
            NullStrategy::Ignore
        } else {
            NullStrategy::Propagate
        };
        let s = self
            .df
            .borrow()
            .mean_horizontal(null_strategy)
            .map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn to_dummies(
        &self,
        columns: Option<Vec<String>>,
        separator: Option<String>,
        drop_first: bool,
    ) -> RbResult<Self> {
        let df = match columns {
            Some(cols) => self.df.borrow().columns_to_dummies(
                cols.iter().map(|x| x as &str).collect(),
                separator.as_deref(),
                drop_first,
            ),
            None => self
                .df
                .borrow()
                .to_dummies(separator.as_deref(), drop_first),
        }
        .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn null_count(&self) -> Self {
        let df = self.df.borrow().null_count();
        df.into()
    }

    pub fn map_rows(
        &self,
        lambda: Value,
        output_type: Option<Wrap<DataType>>,
        inference_size: usize,
    ) -> RbResult<(Value, bool)> {
        let df = &self.df.borrow();

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

        Ok((Obj::wrap(RbSeries::from(out)).as_value(), false))
    }

    pub fn shrink_to_fit(&self) {
        self.df.borrow_mut().shrink_to_fit();
    }

    pub fn hash_rows(&self, k0: u64, k1: u64, k2: u64, k3: u64) -> RbResult<RbSeries> {
        let hb = ahash::RandomState::with_seeds(k0, k1, k2, k3);
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
        offset: String,
        stable: bool,
    ) -> RbResult<Self> {
        let out = if stable {
            self.df.borrow().upsample_stable(
                by,
                &index_column,
                Duration::parse(&every),
                Duration::parse(&offset),
            )
        } else {
            self.df.borrow().upsample(
                by,
                &index_column,
                Duration::parse(&every),
                Duration::parse(&offset),
            )
        };
        let out = out.map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn to_struct(&self, name: String) -> RbSeries {
        let s = self.df.borrow().clone().into_struct(&name);
        s.into_series().into()
    }

    pub fn unnest(&self, names: Vec<String>) -> RbResult<Self> {
        let df = self.df.borrow().unnest(names).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn clear(&self) -> Self {
        self.df.borrow().clear().into()
    }
}

use magnus::{RArray, RHash, Value};
use polars::lazy::frame::{LazyFrame, LazyGroupBy};
use polars::prelude::*;
use std::cell::RefCell;
use std::io::BufWriter;

use crate::conversion::*;
use crate::file::get_file_like;
use crate::lazy::utils::rb_exprs_to_exprs;
use crate::{RbDataFrame, RbExpr, RbPolarsErr, RbResult, RbValueError};

#[magnus::wrap(class = "Polars::RbLazyGroupBy")]
pub struct RbLazyGroupBy {
    lgb: RefCell<Option<LazyGroupBy>>,
}

impl RbLazyGroupBy {
    pub fn agg(&self, aggs: RArray) -> RbResult<RbLazyFrame> {
        let lgb = self.lgb.borrow_mut().take().unwrap();
        let aggs = rb_exprs_to_exprs(aggs)?;
        Ok(lgb.agg(aggs).into())
    }

    pub fn head(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.head(Some(n)).into()
    }

    pub fn tail(&self, n: usize) -> RbLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.tail(Some(n)).into()
    }
}

#[magnus::wrap(class = "Polars::RbLazyFrame")]
#[derive(Clone)]
pub struct RbLazyFrame {
    pub ldf: LazyFrame,
}

impl RbLazyFrame {
    fn get_schema(&self) -> RbResult<SchemaRef> {
        let schema = self.ldf.schema().map_err(RbPolarsErr::from)?;
        Ok(schema)
    }
}

impl From<LazyFrame> for RbLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        RbLazyFrame { ldf }
    }
}

impl RbLazyFrame {
    pub fn write_json(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);
        serde_json::to_writer(file, &self.ldf.logical_plan)
            .map_err(|err| RbValueError::new_err(format!("{:?}", err)))?;
        Ok(())
    }

    pub fn describe_plan(&self) -> String {
        self.ldf.describe_plan()
    }

    pub fn describe_optimized_plan(&self) -> RbResult<String> {
        let result = self
            .ldf
            .describe_optimized_plan()
            .map_err(RbPolarsErr::from)?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn optimization_toggle(
        &self,
        type_coercion: bool,
        predicate_pushdown: bool,
        projection_pushdown: bool,
        simplify_expr: bool,
        slice_pushdown: bool,
        _cse: bool,
        allow_streaming: bool,
    ) -> RbLazyFrame {
        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            // .with_common_subplan_elimination(cse)
            .with_streaming(allow_streaming)
            .with_projection_pushdown(projection_pushdown);
        ldf.into()
    }

    pub fn sort(&self, by_column: String, reverse: bool, nulls_last: bool) -> Self {
        let ldf = self.ldf.clone();
        ldf.sort(
            &by_column,
            SortOptions {
                descending: reverse,
                nulls_last,
            },
        )
        .into()
    }

    pub fn sort_by_exprs(
        &self,
        by_column: RArray,
        reverse: Vec<bool>,
        nulls_last: bool,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let exprs = rb_exprs_to_exprs(by_column)?;
        Ok(ldf.sort_by_exprs(exprs, reverse, nulls_last).into())
    }

    pub fn cache(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.cache().into()
    }

    pub fn collect(&self) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.collect().map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn fetch(&self, n_rows: usize) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.fetch(n_rows).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.filter(predicate.inner.clone()).into()
    }

    pub fn select(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let exprs = rb_exprs_to_exprs(exprs)?;
        Ok(ldf.select(exprs).into())
    }

    pub fn groupby(&self, by: RArray, maintain_order: bool) -> RbResult<RbLazyGroupBy> {
        let ldf = self.ldf.clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = if maintain_order {
            ldf.groupby_stable(by)
        } else {
            ldf.groupby(by)
        };
        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    pub fn groupby_rolling(
        &self,
        index_column: String,
        period: String,
        offset: String,
        closed: String,
        by: RArray,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = wrap_closed_window(&closed)?;
        let ldf = self.ldf.clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = ldf.groupby_rolling(
            by,
            RollingGroupOptions {
                index_column,
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                closed_window,
            },
        );

        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn groupby_dynamic(
        &self,
        index_column: String,
        every: String,
        period: String,
        offset: String,
        truncate: bool,
        include_boundaries: bool,
        closed: String,
        by: RArray,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = wrap_closed_window(&closed)?;
        let by = rb_exprs_to_exprs(by)?;
        let ldf = self.ldf.clone();
        let lazy_gb = ldf.groupby_dynamic(
            by,
            DynamicGroupOptions {
                index_column,
                every: Duration::parse(&every),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                truncate,
                include_boundaries,
                closed_window,
            },
        );

        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join(
        &self,
        other: &RbLazyFrame,
        left_on: RArray,
        right_on: RArray,
        allow_parallel: bool,
        force_parallel: bool,
        how: String,
        suffix: String,
    ) -> RbResult<Self> {
        let how = wrap_join_type(&how)?;

        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = rb_exprs_to_exprs(left_on)?;
        let right_on = rb_exprs_to_exprs(right_on)?;

        Ok(ldf
            .join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(how)
            .suffix(suffix)
            .finish()
            .into())
    }

    pub fn with_columns(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        Ok(ldf.with_columns(rb_exprs_to_exprs(exprs)?).into())
    }

    pub fn rename(&self, existing: Vec<String>, new: Vec<String>) -> Self {
        let ldf = self.ldf.clone();
        ldf.rename(existing, new).into()
    }

    pub fn reverse(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.reverse().into()
    }

    pub fn shift(&self, periods: i64) -> Self {
        let ldf = self.ldf.clone();
        ldf.shift(periods).into()
    }

    pub fn shift_and_fill(&self, periods: i64, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.shift_and_fill(periods, fill_value.inner.clone()).into()
    }

    pub fn fill_nan(&self, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.clone();
        ldf.fill_nan(fill_value.inner.clone()).into()
    }

    pub fn min(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.min().into()
    }

    pub fn max(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.max().into()
    }

    pub fn sum(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.sum().into()
    }

    pub fn mean(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.mean().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        let ldf = self.ldf.clone();
        ldf.std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        let ldf = self.ldf.clone();
        ldf.var(ddof).into()
    }

    pub fn median(&self) -> Self {
        let ldf = self.ldf.clone();
        ldf.median().into()
    }

    pub fn quantile(&self, quantile: f64, interpolation: Wrap<QuantileInterpolOptions>) -> Self {
        let ldf = self.ldf.clone();
        ldf.quantile(quantile, interpolation.0).into()
    }

    pub fn explode(&self, column: RArray) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let column = rb_exprs_to_exprs(column)?;
        Ok(ldf.explode(column).into())
    }

    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<Vec<String>>,
        keep: String,
    ) -> RbResult<Self> {
        let keep = wrap_unique_keep_strategy(&keep)?;
        let ldf = self.ldf.clone();
        Ok(match maintain_order {
            true => ldf.unique_stable(subset, keep),
            false => ldf.unique(subset, keep),
        }
        .into())
    }

    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> Self {
        let ldf = self.ldf.clone();
        ldf.drop_nulls(subset.map(|v| v.into_iter().map(|s| col(&s)).collect()))
            .into()
    }

    pub fn slice(&self, offset: i64, len: Option<IdxSize>) -> Self {
        let ldf = self.ldf.clone();
        ldf.slice(offset, len.unwrap_or(IdxSize::MAX)).into()
    }

    pub fn tail(&self, n: IdxSize) -> Self {
        let ldf = self.ldf.clone();
        ldf.tail(n).into()
    }

    pub fn melt(
        &self,
        id_vars: Vec<String>,
        value_vars: Vec<String>,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> Self {
        let args = MeltArgs {
            id_vars,
            value_vars,
            value_name,
            variable_name,
        };

        let ldf = self.ldf.clone();
        ldf.melt(args).into()
    }

    pub fn with_row_count(&self, name: String, offset: Option<IdxSize>) -> Self {
        let ldf = self.ldf.clone();
        ldf.with_row_count(&name, offset).into()
    }

    pub fn drop_columns(&self, cols: Vec<String>) -> Self {
        let ldf = self.ldf.clone();
        ldf.drop_columns(cols).into()
    }

    pub fn clone(&self) -> Self {
        self.ldf.clone().into()
    }

    pub fn columns(&self) -> RbResult<Vec<String>> {
        Ok(self.get_schema()?.iter_names().cloned().collect())
    }

    pub fn dtypes(&self) -> RbResult<Vec<String>> {
        let schema = self.get_schema()?;
        let iter = schema.iter_dtypes().map(|dt| dt.to_string());
        Ok(iter.collect())
    }

    pub fn schema(&self) -> RbResult<RHash> {
        let schema = self.get_schema()?;
        let schema_dict = RHash::new();

        schema.iter_fields().for_each(|fld| {
            // TODO remove unwrap
            schema_dict
                .aset(fld.name().clone(), fld.data_type().to_string())
                .unwrap();
        });
        Ok(schema_dict)
    }

    pub fn unnest(&self, cols: Vec<String>) -> Self {
        self.ldf.clone().unnest(cols).into()
    }

    pub fn width(&self) -> RbResult<usize> {
        Ok(self.get_schema()?.len())
    }
}

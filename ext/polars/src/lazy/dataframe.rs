use magnus::{RArray, RHash, Value};
use polars::io::RowCount;
use polars::lazy::frame::{LazyFrame, LazyGroupBy};
use polars::prelude::*;
use std::cell::RefCell;
use std::io::{BufWriter, Read};

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
    pub fn read_json(rb_f: Value) -> RbResult<Self> {
        // it is faster to first read to memory and then parse: https://github.com/serde-rs/json/issues/160
        // so don't bother with files.
        let mut json = String::new();
        let _ = get_file_like(rb_f, false)?
            .read_to_string(&mut json)
            .unwrap();

        // Safety
        // we skipped the serializing/deserializing of the static in lifetime in `DataType`
        // so we actually don't have a lifetime at all when serializing.

        // &str still has a lifetime. Bit its ok, because we drop it immediately
        // in this scope
        let json = unsafe { std::mem::transmute::<&'_ str, &'static str>(json.as_str()) };

        let lp = serde_json::from_str::<LogicalPlan>(json)
            .map_err(|err| RbValueError::new_err(format!("{:?}", err)))?;
        Ok(LazyFrame::from(lp).into())
    }

    pub fn new_from_ndjson(
        path: String,
        infer_schema_length: Option<usize>,
        batch_size: Option<usize>,
        n_rows: Option<usize>,
        low_memory: bool,
        rechunk: bool,
        row_count: Option<(String, IdxSize)>,
    ) -> RbResult<Self> {
        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });

        let lf = LazyJsonLineReader::new(path)
            .with_infer_schema_length(infer_schema_length)
            .with_batch_size(batch_size)
            .with_n_rows(n_rows)
            .low_memory(low_memory)
            .with_rechunk(rechunk)
            .with_row_count(row_count)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn new_from_csv(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let path: String = arguments[0].try_convert()?;
        let sep: String = arguments[1].try_convert()?;
        let has_header: bool = arguments[2].try_convert()?;
        let ignore_errors: bool = arguments[3].try_convert()?;
        let skip_rows: usize = arguments[4].try_convert()?;
        let n_rows: Option<usize> = arguments[5].try_convert()?;
        let cache: bool = arguments[6].try_convert()?;
        let overwrite_dtype: Option<Vec<(String, Wrap<DataType>)>> = arguments[7].try_convert()?;
        let low_memory: bool = arguments[8].try_convert()?;
        let comment_char: Option<String> = arguments[9].try_convert()?;
        let quote_char: Option<String> = arguments[10].try_convert()?;
        let null_values: Option<Wrap<NullValues>> = arguments[11].try_convert()?;
        let infer_schema_length: Option<usize> = arguments[12].try_convert()?;
        let with_schema_modify: Option<Value> = arguments[13].try_convert()?;
        let rechunk: bool = arguments[14].try_convert()?;
        let skip_rows_after_header: usize = arguments[15].try_convert()?;
        let encoding: Wrap<CsvEncoding> = arguments[16].try_convert()?;
        let row_count: Option<(String, IdxSize)> = arguments[17].try_convert()?;
        let parse_dates: bool = arguments[18].try_convert()?;
        let eol_char: String = arguments[19].try_convert()?;
        // end arguments

        let null_values = null_values.map(|w| w.0);
        let comment_char = comment_char.map(|s| s.as_bytes()[0]);
        let quote_char = quote_char.map(|s| s.as_bytes()[0]);
        let delimiter = sep.as_bytes()[0];
        let eol_char = eol_char.as_bytes()[0];

        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });

        let overwrite_dtype = overwrite_dtype.map(|overwrite_dtype| {
            let fields = overwrite_dtype
                .into_iter()
                .map(|(name, dtype)| Field::new(&name, dtype.0));
            Schema::from(fields)
        });
        let r = LazyCsvReader::new(path)
            .with_infer_schema_length(infer_schema_length)
            .with_delimiter(delimiter)
            .has_header(has_header)
            .with_ignore_parser_errors(ignore_errors)
            .with_skip_rows(skip_rows)
            .with_n_rows(n_rows)
            .with_cache(cache)
            .with_dtype_overwrite(overwrite_dtype.as_ref())
            .low_memory(low_memory)
            .with_comment_char(comment_char)
            .with_quote_char(quote_char)
            .with_end_of_line_char(eol_char)
            .with_rechunk(rechunk)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_encoding(encoding.0)
            .with_row_count(row_count)
            .with_parse_dates(parse_dates)
            .with_null_values(null_values);

        if let Some(_lambda) = with_schema_modify {
            todo!();
        }

        Ok(r.finish().map_err(RbPolarsErr::from)?.into())
    }

    pub fn new_from_parquet(
        path: String,
        n_rows: Option<usize>,
        cache: bool,
        parallel: Wrap<ParallelStrategy>,
        rechunk: bool,
        row_count: Option<(String, IdxSize)>,
        low_memory: bool,
    ) -> RbResult<Self> {
        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });
        let args = ScanArgsParquet {
            n_rows,
            cache,
            parallel: parallel.0,
            rechunk,
            row_count,
            low_memory,
        };
        let lf = LazyFrame::scan_parquet(path, args).map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn new_from_ipc(
        path: String,
        n_rows: Option<usize>,
        cache: bool,
        rechunk: bool,
        row_count: Option<(String, IdxSize)>,
        memory_map: bool,
    ) -> RbResult<Self> {
        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });
        let args = ScanArgsIpc {
            n_rows,
            cache,
            rechunk,
            row_count,
            memmap: memory_map,
        };
        let lf = LazyFrame::scan_ipc(path, args).map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

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
        cse: bool,
        allow_streaming: bool,
    ) -> RbLazyFrame {
        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            .with_common_subplan_elimination(cse)
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
        closed: Wrap<ClosedWindow>,
        by: RArray,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = closed.0;
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
        closed: Wrap<ClosedWindow>,
        by: RArray,
        start_by: Wrap<StartBy>,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = closed.0;
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
                start_by: start_by.0,
            },
        );

        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    pub fn with_context(&self, contexts: RArray) -> RbResult<Self> {
        let contexts = contexts
            .each()
            .map(|v| v.unwrap().try_convert())
            .collect::<RbResult<Vec<&RbLazyFrame>>>()?;
        let contexts = contexts
            .into_iter()
            .map(|ldf| ldf.ldf.clone())
            .collect::<Vec<_>>();
        Ok(self.ldf.clone().with_context(contexts).into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join_asof(
        &self,
        other: &RbLazyFrame,
        left_on: &RbExpr,
        right_on: &RbExpr,
        left_by: Option<Vec<String>>,
        right_by: Option<Vec<String>>,
        allow_parallel: bool,
        force_parallel: bool,
        suffix: String,
        strategy: Wrap<AsofStrategy>,
        tolerance: Option<Wrap<AnyValue<'_>>>,
        tolerance_str: Option<String>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = left_on.inner.clone();
        let right_on = right_on.inner.clone();
        Ok(ldf
            .join_builder()
            .with(other)
            .left_on([left_on])
            .right_on([right_on])
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(JoinType::AsOf(AsOfOptions {
                strategy: strategy.0,
                left_by,
                right_by,
                tolerance: tolerance.map(|t| t.0.into_static().unwrap()),
                tolerance_str,
            }))
            .suffix(suffix)
            .finish()
            .into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join(
        &self,
        other: &RbLazyFrame,
        left_on: RArray,
        right_on: RArray,
        allow_parallel: bool,
        force_parallel: bool,
        how: Wrap<JoinType>,
        suffix: String,
    ) -> RbResult<Self> {
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
            .how(how.0)
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

    pub fn quantile(
        &self,
        quantile: &RbExpr,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> Self {
        let ldf = self.ldf.clone();
        ldf.quantile(quantile.inner.clone(), interpolation.0).into()
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
        keep: Wrap<UniqueKeepStrategy>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.clone();
        Ok(match maintain_order {
            true => ldf.unique_stable(subset, keep.0),
            false => ldf.unique(subset, keep.0),
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

    pub fn dtypes(&self) -> RbResult<Vec<Value>> {
        let schema = self.get_schema()?;
        let iter = schema.iter_dtypes().map(|dt| Wrap(dt.clone()).into());
        Ok(iter.collect())
    }

    pub fn schema(&self) -> RbResult<RHash> {
        let schema = self.get_schema()?;
        let schema_dict = RHash::new();

        schema.iter_fields().for_each(|fld| {
            // TODO remove unwrap
            schema_dict
                .aset::<String, Value>(fld.name().clone(), Wrap(fld.data_type().clone()).into())
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

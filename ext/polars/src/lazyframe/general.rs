use magnus::{r_hash::ForEach, typed_data::Obj, IntoValue, RArray, RHash, TryConvert, Value};
use polars::io::{HiveOptions, RowIndex};
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use std::cell::RefCell;
use std::io::BufWriter;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use crate::conversion::*;
use crate::expr::rb_exprs_to_exprs;
use crate::file::get_file_like;
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbLazyGroupBy, RbPolarsErr, RbResult, RbValueError};

impl RbLazyFrame {
    pub fn new_from_ndjson(
        path: String,
        infer_schema_length: Option<usize>,
        batch_size: Option<Wrap<NonZeroUsize>>,
        n_rows: Option<usize>,
        low_memory: bool,
        rechunk: bool,
        row_index: Option<(String, IdxSize)>,
    ) -> RbResult<Self> {
        let batch_size = batch_size.map(|v| v.0);
        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });

        let lf = LazyJsonLineReader::new(path)
            .with_infer_schema_length(infer_schema_length.and_then(NonZeroUsize::new))
            .with_batch_size(batch_size)
            .with_n_rows(n_rows)
            .low_memory(low_memory)
            .with_rechunk(rechunk)
            .with_row_index(row_index)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn new_from_csv(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let path = String::try_convert(arguments[0])?;
        let separator = String::try_convert(arguments[1])?;
        let has_header = bool::try_convert(arguments[2])?;
        let ignore_errors = bool::try_convert(arguments[3])?;
        let skip_rows = usize::try_convert(arguments[4])?;
        let n_rows = Option::<usize>::try_convert(arguments[5])?;
        let cache = bool::try_convert(arguments[6])?;
        let overwrite_dtype = Option::<Vec<(String, Wrap<DataType>)>>::try_convert(arguments[7])?;
        let low_memory = bool::try_convert(arguments[8])?;
        let comment_prefix = Option::<String>::try_convert(arguments[9])?;
        let quote_char = Option::<String>::try_convert(arguments[10])?;
        let null_values = Option::<Wrap<NullValues>>::try_convert(arguments[11])?;
        let infer_schema_length = Option::<usize>::try_convert(arguments[12])?;
        let with_schema_modify = Option::<Value>::try_convert(arguments[13])?;
        let rechunk = bool::try_convert(arguments[14])?;
        let skip_rows_after_header = usize::try_convert(arguments[15])?;
        let encoding = Wrap::<CsvEncoding>::try_convert(arguments[16])?;
        let row_index = Option::<(String, IdxSize)>::try_convert(arguments[17])?;
        let try_parse_dates = bool::try_convert(arguments[18])?;
        let eol_char = String::try_convert(arguments[19])?;
        let truncate_ragged_lines = bool::try_convert(arguments[20])?;
        // end arguments

        let null_values = null_values.map(|w| w.0);
        let quote_char = quote_char.map(|s| s.as_bytes()[0]);
        let separator = separator.as_bytes()[0];
        let eol_char = eol_char.as_bytes()[0];
        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });

        let overwrite_dtype = overwrite_dtype.map(|overwrite_dtype| {
            overwrite_dtype
                .into_iter()
                .map(|(name, dtype)| Field::new((&*name).into(), dtype.0))
                .collect::<Schema>()
        });

        let r = LazyCsvReader::new(path)
            .with_infer_schema_length(infer_schema_length)
            .with_separator(separator)
            .with_has_header(has_header)
            .with_ignore_errors(ignore_errors)
            .with_skip_rows(skip_rows)
            .with_n_rows(n_rows)
            .with_cache(cache)
            .with_dtype_overwrite(overwrite_dtype.map(Arc::new))
            // TODO add with_schema
            .with_low_memory(low_memory)
            .with_comment_prefix(comment_prefix.map(|x| x.into()))
            .with_quote_char(quote_char)
            .with_eol_char(eol_char)
            .with_rechunk(rechunk)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_encoding(encoding.0)
            .with_row_index(row_index)
            .with_try_parse_dates(try_parse_dates)
            .with_null_values(null_values)
            // TODO add with_missing_is_null
            .with_truncate_ragged_lines(truncate_ragged_lines);

        if let Some(_lambda) = with_schema_modify {
            todo!();
        }

        Ok(r.finish().map_err(RbPolarsErr::from)?.into())
    }

    pub fn new_from_parquet(arguments: &[Value]) -> RbResult<Self> {
        let path = Option::<PathBuf>::try_convert(arguments[0])?;
        let paths = Vec::<PathBuf>::try_convert(arguments[1])?;
        let n_rows = Option::<usize>::try_convert(arguments[2])?;
        let cache = bool::try_convert(arguments[3])?;
        let parallel = Wrap::<ParallelStrategy>::try_convert(arguments[4])?;
        let rechunk = bool::try_convert(arguments[5])?;
        let row_index = Option::<(String, IdxSize)>::try_convert(arguments[6])?;
        let low_memory = bool::try_convert(arguments[7])?;
        let use_statistics = bool::try_convert(arguments[8])?;
        let hive_partitioning = Option::<bool>::try_convert(arguments[9])?;
        let schema = Option::<Wrap<Schema>>::try_convert(arguments[10])?;
        let hive_schema = Option::<Wrap<Schema>>::try_convert(arguments[11])?;
        let try_parse_hive_dates = bool::try_convert(arguments[12])?;
        let glob = bool::try_convert(arguments[13])?;
        let include_file_paths = Option::<String>::try_convert(arguments[14])?;
        let allow_missing_columns = bool::try_convert(arguments[15])?;

        let parallel = parallel.0;
        let hive_schema = hive_schema.map(|s| Arc::new(s.0));

        let first_path = if let Some(path) = &path {
            path
        } else {
            paths
                .first()
                .ok_or_else(|| RbValueError::new_err("expected a path argument".to_string()))?
        };

        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });
        let hive_options = HiveOptions {
            enabled: hive_partitioning,
            hive_start_idx: 0,
            schema: hive_schema,
            try_parse_dates: try_parse_hive_dates,
        };

        let args = ScanArgsParquet {
            n_rows,
            cache,
            parallel,
            rechunk,
            row_index,
            low_memory,
            cloud_options: None,
            use_statistics,
            schema: schema.map(|x| Arc::new(x.0)),
            hive_options,
            glob,
            include_file_paths: include_file_paths.map(|x| x.into()),
            allow_missing_columns,
        };

        let lf = if path.is_some() {
            LazyFrame::scan_parquet(first_path, args)
        } else {
            LazyFrame::scan_parquet_files(Arc::from(paths), args)
        }
        .map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_from_ipc(
        path: String,
        n_rows: Option<usize>,
        cache: bool,
        rechunk: bool,
        row_index: Option<(String, IdxSize)>,
        hive_partitioning: Option<bool>,
        hive_schema: Option<Wrap<Schema>>,
        try_parse_hive_dates: bool,
        include_file_paths: Option<String>,
    ) -> RbResult<Self> {
        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });

        let hive_options = HiveOptions {
            enabled: hive_partitioning,
            hive_start_idx: 0,
            schema: hive_schema.map(|x| Arc::new(x.0)),
            try_parse_dates: try_parse_hive_dates,
        };

        let args = ScanArgsIpc {
            n_rows,
            cache,
            rechunk,
            row_index,
            cloud_options: None,
            hive_options,
            include_file_paths: include_file_paths.map(|x| x.into()),
        };
        let lf = LazyFrame::scan_ipc(path, args).map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn write_json(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);
        serde_json::to_writer(file, &self.ldf.borrow().logical_plan)
            .map_err(|err| RbValueError::new_err(format!("{:?}", err)))?;
        Ok(())
    }

    pub fn describe_plan(&self) -> RbResult<String> {
        self.ldf
            .borrow()
            .describe_plan()
            .map_err(RbPolarsErr::from)
            .map_err(Into::into)
    }

    pub fn describe_optimized_plan(&self) -> RbResult<String> {
        let result = self
            .ldf
            .borrow()
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
        comm_subplan_elim: bool,
        comm_subexpr_elim: bool,
        allow_streaming: bool,
        _eager: bool,
    ) -> RbLazyFrame {
        let ldf = self.ldf.borrow().clone();
        let mut ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            .with_streaming(allow_streaming)
            ._with_eager(_eager)
            .with_projection_pushdown(projection_pushdown);

        ldf = ldf.with_comm_subplan_elim(comm_subplan_elim);
        ldf = ldf.with_comm_subexpr_elim(comm_subexpr_elim);

        ldf.into()
    }

    pub fn sort(
        &self,
        by_column: String,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
        multithreaded: bool,
    ) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.sort(
            [&by_column],
            SortMultipleOptions {
                descending: vec![descending],
                nulls_last: vec![nulls_last],
                multithreaded,
                maintain_order,
            },
        )
        .into()
    }

    pub fn sort_by_exprs(
        &self,
        by: RArray,
        descending: Vec<bool>,
        nulls_last: Vec<bool>,
        maintain_order: bool,
        multithreaded: bool,
    ) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let exprs = rb_exprs_to_exprs(by)?;
        Ok(ldf
            .sort_by_exprs(
                exprs,
                SortMultipleOptions {
                    descending,
                    nulls_last,
                    maintain_order,
                    multithreaded,
                },
            )
            .into())
    }

    pub fn cache(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.cache().into()
    }

    pub fn collect(&self) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.borrow().clone();
        let df = ldf.collect().map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sink_parquet(
        &self,
        path: PathBuf,
        compression: String,
        compression_level: Option<i32>,
        statistics: Wrap<StatisticsOptions>,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
        maintain_order: bool,
    ) -> RbResult<()> {
        let compression = parse_parquet_compression(&compression, compression_level)?;

        let options = ParquetWriteOptions {
            compression,
            statistics: statistics.0,
            row_group_size,
            data_page_size,
            maintain_order,
        };

        let ldf = self.ldf.borrow().clone();
        ldf.sink_parquet(path, options).map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn sink_ipc(
        &self,
        path: PathBuf,
        compression: Option<Wrap<IpcCompression>>,
        maintain_order: bool,
    ) -> RbResult<()> {
        let options = IpcWriterOptions {
            compression: compression.map(|c| c.0),
            maintain_order,
        };

        let ldf = self.ldf.borrow().clone();
        ldf.sink_ipc(path, options).map_err(RbPolarsErr::from)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sink_csv(
        &self,
        path: PathBuf,
        include_bom: bool,
        include_header: bool,
        separator: u8,
        line_terminator: String,
        quote_char: u8,
        batch_size: Wrap<NonZeroUsize>,
        datetime_format: Option<String>,
        date_format: Option<String>,
        time_format: Option<String>,
        float_scientific: Option<bool>,
        float_precision: Option<usize>,
        null_value: Option<String>,
        quote_style: Option<Wrap<QuoteStyle>>,
        maintain_order: bool,
    ) -> RbResult<()> {
        let quote_style = quote_style.map_or(QuoteStyle::default(), |wrap| wrap.0);
        let null_value = null_value.unwrap_or(SerializeOptions::default().null);

        let serialize_options = SerializeOptions {
            date_format,
            time_format,
            datetime_format,
            float_scientific,
            float_precision,
            separator,
            quote_char,
            null: null_value,
            line_terminator,
            quote_style,
        };

        let options = CsvWriterOptions {
            include_bom,
            include_header,
            maintain_order,
            batch_size: batch_size.0,
            serialize_options,
        };

        let ldf = self.ldf.borrow().clone();
        ldf.sink_csv(path, options).map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn sink_json(&self, path: PathBuf, maintain_order: bool) -> RbResult<()> {
        let options = JsonWriterOptions { maintain_order };

        let ldf = self.ldf.borrow().clone();
        ldf.sink_json(path, options).map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn fetch(&self, n_rows: usize) -> RbResult<RbDataFrame> {
        let ldf = self.ldf.borrow().clone();
        let df = ldf.fetch(n_rows).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.filter(predicate.inner.clone()).into()
    }

    pub fn select(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let exprs = rb_exprs_to_exprs(exprs)?;
        Ok(ldf.select(exprs).into())
    }

    pub fn select_seq(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let exprs = rb_exprs_to_exprs(exprs)?;
        Ok(ldf.select_seq(exprs).into())
    }

    pub fn group_by(&self, by: RArray, maintain_order: bool) -> RbResult<RbLazyGroupBy> {
        let ldf = self.ldf.borrow().clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = if maintain_order {
            ldf.group_by_stable(by)
        } else {
            ldf.group_by(by)
        };
        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    pub fn rolling(
        &self,
        index_column: &RbExpr,
        period: String,
        offset: String,
        closed: Wrap<ClosedWindow>,
        by: RArray,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = closed.0;
        let ldf = self.ldf.borrow().clone();
        let by = rb_exprs_to_exprs(by)?;
        let lazy_gb = ldf.rolling(
            index_column.inner.clone(),
            by,
            RollingGroupOptions {
                index_column: "".into(),
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
    pub fn group_by_dynamic(
        &self,
        index_column: &RbExpr,
        every: String,
        period: String,
        offset: String,
        label: Wrap<Label>,
        include_boundaries: bool,
        closed: Wrap<ClosedWindow>,
        by: RArray,
        start_by: Wrap<StartBy>,
    ) -> RbResult<RbLazyGroupBy> {
        let closed_window = closed.0;
        let by = rb_exprs_to_exprs(by)?;
        let ldf = self.ldf.borrow().clone();
        let lazy_gb = ldf.group_by_dynamic(
            index_column.inner.clone(),
            by,
            DynamicGroupOptions {
                every: Duration::parse(&every),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                label: label.0,
                include_boundaries,
                closed_window,
                start_by: start_by.0,
                ..Default::default()
            },
        );

        Ok(RbLazyGroupBy {
            lgb: RefCell::new(Some(lazy_gb)),
        })
    }

    pub fn with_context(&self, contexts: RArray) -> RbResult<Self> {
        let contexts = contexts.typecheck::<Obj<RbLazyFrame>>()?;
        let contexts = contexts
            .into_iter()
            .map(|ldf| ldf.ldf.borrow().clone())
            .collect::<Vec<_>>();
        Ok(self.ldf.borrow().clone().with_context(contexts).into())
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
        coalesce: bool,
    ) -> RbResult<Self> {
        let coalesce = if coalesce {
            JoinCoalesce::CoalesceColumns
        } else {
            JoinCoalesce::KeepColumns
        };
        let ldf = self.ldf.borrow().clone();
        let other = other.ldf.borrow().clone();
        let left_on = left_on.inner.clone();
        let right_on = right_on.inner.clone();
        Ok(ldf
            .join_builder()
            .with(other)
            .left_on([left_on])
            .right_on([right_on])
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .coalesce(coalesce)
            .how(JoinType::AsOf(AsOfOptions {
                strategy: strategy.0,
                left_by: left_by.map(strings_to_pl_smallstr),
                right_by: right_by.map(strings_to_pl_smallstr),
                tolerance: tolerance.map(|t| t.0.into_static()),
                tolerance_str: tolerance_str.map(|s| s.into()),
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
        join_nulls: bool,
        how: Wrap<JoinType>,
        suffix: String,
        validate: Wrap<JoinValidation>,
        coalesce: Option<bool>,
    ) -> RbResult<Self> {
        let coalesce = match coalesce {
            None => JoinCoalesce::JoinSpecific,
            Some(true) => JoinCoalesce::CoalesceColumns,
            Some(false) => JoinCoalesce::KeepColumns,
        };
        let ldf = self.ldf.borrow().clone();
        let other = other.ldf.borrow().clone();
        let left_on = rb_exprs_to_exprs(left_on)?;
        let right_on = rb_exprs_to_exprs(right_on)?;

        Ok(ldf
            .join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .join_nulls(join_nulls)
            .how(how.0)
            .validate(validate.0)
            .coalesce(coalesce)
            .suffix(suffix)
            .finish()
            .into())
    }

    pub fn with_column(&self, expr: &RbExpr) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.with_column(expr.inner.clone()).into()
    }

    pub fn with_columns(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        Ok(ldf.with_columns(rb_exprs_to_exprs(exprs)?).into())
    }

    pub fn with_columns_seq(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        Ok(ldf.with_columns_seq(rb_exprs_to_exprs(exprs)?).into())
    }

    pub fn rename(&self, existing: Vec<String>, new: Vec<String>, strict: bool) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.rename(existing, new, strict).into()
    }

    pub fn reverse(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.reverse().into()
    }

    pub fn shift(&self, n: &RbExpr, fill_value: Option<&RbExpr>) -> Self {
        let lf = self.ldf.borrow().clone();
        let out = match fill_value {
            Some(v) => lf.shift_and_fill(n.inner.clone(), v.inner.clone()),
            None => lf.shift(n.inner.clone()),
        };
        out.into()
    }

    pub fn fill_nan(&self, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.fill_nan(fill_value.inner.clone()).into()
    }

    pub fn min(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.min();
        out.into()
    }

    pub fn max(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.max();
        out.into()
    }

    pub fn sum(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.sum();
        out.into()
    }

    pub fn mean(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.mean();
        out.into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.std(ddof);
        out.into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.var(ddof);
        out.into()
    }

    pub fn median(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.median();
        out.into()
    }

    pub fn quantile(&self, quantile: &RbExpr, interpolation: Wrap<QuantileMethod>) -> Self {
        let ldf = self.ldf.borrow().clone();
        let out = ldf.quantile(quantile.inner.clone(), interpolation.0);
        out.into()
    }

    pub fn explode(&self, column: RArray) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let column = rb_exprs_to_exprs(column)?;
        Ok(ldf.explode(column).into())
    }

    pub fn null_count(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.null_count().into()
    }

    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<Vec<String>>,
        keep: Wrap<UniqueKeepStrategy>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        Ok(match maintain_order {
            true => ldf.unique_stable_generic(subset, keep.0),
            false => ldf.unique_generic(subset, keep.0),
        }
        .into())
    }

    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.drop_nulls(subset.map(|v| v.into_iter().map(|s| col(&s)).collect()))
            .into()
    }

    pub fn slice(&self, offset: i64, len: Option<IdxSize>) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.slice(offset, len.unwrap_or(IdxSize::MAX)).into()
    }

    pub fn tail(&self, n: IdxSize) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.tail(n).into()
    }

    pub fn unpivot(
        &self,
        on: RArray,
        index: RArray,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> RbResult<Self> {
        let on = rb_exprs_to_exprs(on)?;
        let index = rb_exprs_to_exprs(index)?;
        let args = UnpivotArgsDSL {
            on: on.into_iter().map(|e| e.into()).collect(),
            index: index.into_iter().map(|e| e.into()).collect(),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
        };

        let ldf = self.ldf.borrow().clone();
        Ok(ldf.unpivot(args).into())
    }

    pub fn with_row_index(&self, name: String, offset: Option<IdxSize>) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.with_row_index(&name, offset).into()
    }

    pub fn drop(&self, cols: Vec<String>) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.drop(cols).into()
    }

    pub fn cast(&self, rb_dtypes: RHash, strict: bool) -> RbResult<Self> {
        let mut dtypes = Vec::new();
        rb_dtypes.foreach(|k: String, v: Wrap<DataType>| {
            dtypes.push((k, v.0));
            Ok(ForEach::Continue)
        })?;
        let mut cast_map = PlHashMap::with_capacity(dtypes.len());
        cast_map.extend(dtypes.iter().map(|(k, v)| (k.as_ref(), v.clone())));
        Ok(self.ldf.borrow().clone().cast(cast_map, strict).into())
    }

    pub fn cast_all(&self, dtype: Wrap<DataType>, strict: bool) -> Self {
        self.ldf.borrow().clone().cast_all(dtype.0, strict).into()
    }

    pub fn clone(&self) -> Self {
        self.ldf.borrow().clone().into()
    }

    pub fn collect_schema(&self) -> RbResult<RHash> {
        let schema = self
            .ldf
            .borrow_mut()
            .collect_schema()
            .map_err(RbPolarsErr::from)?;

        let schema_dict = RHash::new();
        schema.iter_fields().for_each(|fld| {
            schema_dict
                .aset::<String, Value>(
                    fld.name().to_string(),
                    Wrap(fld.dtype().clone()).into_value(),
                )
                .unwrap();
        });
        Ok(schema_dict)
    }

    pub fn unnest(&self, cols: Vec<String>) -> Self {
        self.ldf.borrow().clone().unnest(cols).into()
    }

    pub fn count(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.count().into()
    }

    pub fn merge_sorted(&self, other: &Self, key: String) -> RbResult<Self> {
        let out = self
            .ldf
            .borrow()
            .clone()
            .merge_sorted(other.ldf.borrow().clone(), &key)
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }
}

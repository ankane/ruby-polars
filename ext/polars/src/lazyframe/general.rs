use magnus::{
    IntoValue, RArray, RHash, Ruby, TryConvert, Value, r_hash::ForEach,
    try_convert::TryConvertOwned, typed_data::Obj,
};
use polars::io::RowIndex;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use polars_plan::dsl::ScanSources;
use std::cell::RefCell;
use std::io::BufWriter;
use std::num::NonZeroUsize;

use super::SinkTarget;
use crate::conversion::*;
use crate::expr::rb_exprs_to_exprs;
use crate::expr::selector::RbSelector;
use crate::file::get_file_like;
use crate::io::RbScanOptions;
use crate::{RbDataFrame, RbExpr, RbLazyFrame, RbLazyGroupBy, RbPolarsErr, RbResult, RbValueError};

fn rbobject_to_first_path_and_scan_sources(obj: Value) -> RbResult<(Option<PlPath>, ScanSources)> {
    use crate::file::{RubyScanSourceInput, get_ruby_scan_source_input};
    Ok(match get_ruby_scan_source_input(obj, false)? {
        RubyScanSourceInput::Path(path) => (
            Some(path.clone()),
            ScanSources::Paths(FromIterator::from_iter([path])),
        ),
        RubyScanSourceInput::File(file) => (None, ScanSources::Files([file].into())),
        RubyScanSourceInput::Buffer(buff) => (None, ScanSources::Buffers([buff].into())),
    })
}

impl RbLazyFrame {
    #[allow(clippy::too_many_arguments)]
    pub fn new_from_ndjson(arguments: &[Value]) -> RbResult<Self> {
        let source = Option::<Value>::try_convert(arguments[0])?;
        let sources = Wrap::<ScanSources>::try_convert(arguments[1])?;
        let infer_schema_length = Option::<usize>::try_convert(arguments[2])?;
        let schema = Option::<Wrap<Schema>>::try_convert(arguments[3])?;
        let schema_overrides = Option::<Wrap<Schema>>::try_convert(arguments[4])?;
        let batch_size = Option::<NonZeroUsize>::try_convert(arguments[5])?;
        let n_rows = Option::<usize>::try_convert(arguments[6])?;
        let low_memory = bool::try_convert(arguments[7])?;
        let rechunk = bool::try_convert(arguments[8])?;
        let row_index = Option::<(String, IdxSize)>::try_convert(arguments[9])?;
        let ignore_errors = bool::try_convert(arguments[10])?;
        let include_file_paths = Option::<String>::try_convert(arguments[11])?;
        let cloud_options = Option::<Vec<(String, String)>>::try_convert(arguments[12])?;
        let credential_provider = Option::<Value>::try_convert(arguments[13])?;
        let retries = usize::try_convert(arguments[14])?;
        let file_cache_ttl = Option::<u64>::try_convert(arguments[15])?;

        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });

        let sources = sources.0;
        let (first_path, sources) = match source {
            None => (sources.first_path().map(|p| p.into_owned()), sources),
            Some(source) => rbobject_to_first_path_and_scan_sources(source)?,
        };

        let mut r = LazyJsonLineReader::new_with_sources(sources);

        if let Some(first_path) = first_path {
            let first_path_url = first_path.to_str();

            let mut cloud_options =
                parse_cloud_options(first_path_url, cloud_options.unwrap_or_default())?;
            cloud_options = cloud_options
                .with_max_retries(retries)
                .with_credential_provider(credential_provider.map(|_| todo!()));

            if let Some(file_cache_ttl) = file_cache_ttl {
                cloud_options.file_cache_ttl = file_cache_ttl;
            }

            r = r.with_cloud_options(Some(cloud_options));
        };

        let lf = r
            .with_infer_schema_length(infer_schema_length.and_then(NonZeroUsize::new))
            .with_batch_size(batch_size)
            .with_n_rows(n_rows)
            .low_memory(low_memory)
            .with_rechunk(rechunk)
            .with_schema(schema.map(|schema| Arc::new(schema.0)))
            .with_schema_overwrite(schema_overrides.map(|x| Arc::new(x.0)))
            .with_row_index(row_index)
            .with_ignore_errors(ignore_errors)
            .with_include_file_paths(include_file_paths.map(|x| x.into()))
            .finish()
            .map_err(RbPolarsErr::from)?;

        Ok(lf.into())
    }

    pub fn new_from_csv(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let source = Option::<Value>::try_convert(arguments[0])?;
        let sources = Wrap::<ScanSources>::try_convert(arguments[1])?;
        let separator = String::try_convert(arguments[2])?;
        let has_header = bool::try_convert(arguments[3])?;
        let ignore_errors = bool::try_convert(arguments[4])?;
        let skip_rows = usize::try_convert(arguments[5])?;
        let skip_lines = usize::try_convert(arguments[6])?;
        let n_rows = Option::<usize>::try_convert(arguments[7])?;
        let cache = bool::try_convert(arguments[8])?;
        let overwrite_dtype = Option::<Vec<(String, Wrap<DataType>)>>::try_convert(arguments[9])?;
        let low_memory = bool::try_convert(arguments[10])?;
        let comment_prefix = Option::<String>::try_convert(arguments[11])?;
        let quote_char = Option::<String>::try_convert(arguments[12])?;
        let null_values = Option::<Wrap<NullValues>>::try_convert(arguments[13])?;
        let missing_utf8_is_empty_string = bool::try_convert(arguments[14])?;
        let infer_schema_length = Option::<usize>::try_convert(arguments[15])?;
        let with_schema_modify = Option::<Value>::try_convert(arguments[16])?;
        let rechunk = bool::try_convert(arguments[17])?;
        let skip_rows_after_header = usize::try_convert(arguments[18])?;
        let encoding = Wrap::<CsvEncoding>::try_convert(arguments[19])?;
        let row_index = Option::<(String, IdxSize)>::try_convert(arguments[20])?;
        let try_parse_dates = bool::try_convert(arguments[21])?;
        let eol_char = String::try_convert(arguments[22])?;
        let raise_if_empty = bool::try_convert(arguments[23])?;
        let truncate_ragged_lines = bool::try_convert(arguments[24])?;
        let decimal_comma = bool::try_convert(arguments[25])?;
        let glob = bool::try_convert(arguments[26])?;
        let schema = Option::<Wrap<Schema>>::try_convert(arguments[27])?;
        let cloud_options = Option::<Vec<(String, String)>>::try_convert(arguments[28])?;
        let _credential_provider = Option::<Value>::try_convert(arguments[29])?;
        let retries = usize::try_convert(arguments[30])?;
        let file_cache_ttl = Option::<u64>::try_convert(arguments[31])?;
        let include_file_paths = Option::<String>::try_convert(arguments[32])?;
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

        let sources = sources.0;
        let (first_path, sources) = match source {
            None => (sources.first_path().map(|p| p.into_owned()), sources),
            Some(source) => rbobject_to_first_path_and_scan_sources(source)?,
        };

        let mut r = LazyCsvReader::new_with_sources(sources);

        if let Some(first_path) = first_path {
            let first_path_url = first_path.to_str();

            let mut cloud_options =
                parse_cloud_options(first_path_url, cloud_options.unwrap_or_default())?;
            if let Some(file_cache_ttl) = file_cache_ttl {
                cloud_options.file_cache_ttl = file_cache_ttl;
            }
            cloud_options = cloud_options.with_max_retries(retries);
            r = r.with_cloud_options(Some(cloud_options));
        }

        let r = r
            .with_infer_schema_length(infer_schema_length)
            .with_separator(separator)
            .with_has_header(has_header)
            .with_ignore_errors(ignore_errors)
            .with_skip_rows(skip_rows)
            .with_skip_lines(skip_lines)
            .with_n_rows(n_rows)
            .with_cache(cache)
            .with_dtype_overwrite(overwrite_dtype.map(Arc::new))
            .with_schema(schema.map(|schema| Arc::new(schema.0)))
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
            .with_missing_is_null(!missing_utf8_is_empty_string)
            .with_truncate_ragged_lines(truncate_ragged_lines)
            .with_decimal_comma(decimal_comma)
            .with_glob(glob)
            .with_raise_if_empty(raise_if_empty)
            .with_include_file_paths(include_file_paths.map(|x| x.into()));

        if let Some(_lambda) = with_schema_modify {
            todo!();
        }

        Ok(r.finish().map_err(RbPolarsErr::from)?.into())
    }

    pub fn new_from_parquet(
        sources: Wrap<ScanSources>,
        schema: Option<Wrap<Schema>>,
        scan_options: RbScanOptions,
        parallel: Wrap<ParallelStrategy>,
        low_memory: bool,
        use_statistics: bool,
    ) -> RbResult<Self> {
        use crate::utils::to_rb_err;

        let parallel = parallel.0;

        let options = ParquetOptions {
            schema: schema.map(|x| Arc::new(x.0)),
            parallel,
            low_memory,
            use_statistics,
        };

        let sources = sources.0;
        let first_path = sources.first_path().map(|p| p.into_owned());

        let unified_scan_args =
            scan_options.extract_unified_scan_args(first_path.as_ref().map(|p| p.as_ref()))?;

        let lf: LazyFrame = DslBuilder::scan_parquet(sources, options, unified_scan_args)
            .map_err(to_rb_err)?
            .build()
            .into();

        Ok(lf.into())
    }

    pub fn new_from_ipc(
        sources: Wrap<ScanSources>,
        scan_options: RbScanOptions,
        file_cache_ttl: Option<u64>,
    ) -> RbResult<Self> {
        let options = IpcScanOptions;

        let sources = sources.0;
        let first_path = sources.first_path().map(|p| p.into_owned());

        let mut unified_scan_args =
            scan_options.extract_unified_scan_args(first_path.as_ref().map(|p| p.as_ref()))?;

        if let Some(file_cache_ttl) = file_cache_ttl {
            unified_scan_args
                .cloud_options
                .get_or_insert_default()
                .file_cache_ttl = file_cache_ttl;
        }

        let lf = LazyFrame::scan_ipc_sources(sources, options, unified_scan_args)
            .map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn write_json(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);
        serde_json::to_writer(file, &self.ldf.borrow().logical_plan)
            .map_err(|err| RbValueError::new_err(format!("{err:?}")))?;
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
            .with_new_streaming(allow_streaming)
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
                limit: None,
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
                    limit: None,
                },
            )
            .into())
    }

    pub fn top_k(&self, k: IdxSize, by: RArray, reverse: Vec<bool>) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let exprs = rb_exprs_to_exprs(by)?;
        Ok(ldf
            .top_k(
                k,
                exprs,
                SortMultipleOptions::new().with_order_descending_multi(reverse),
            )
            .into())
    }

    pub fn bottom_k(&self, k: IdxSize, by: RArray, reverse: Vec<bool>) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let exprs = rb_exprs_to_exprs(by)?;
        Ok(ldf
            .bottom_k(
                k,
                exprs,
                SortMultipleOptions::new().with_order_descending_multi(reverse),
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
        target: SinkTarget,
        compression: String,
        compression_level: Option<i32>,
        statistics: Wrap<StatisticsOptions>,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
        cloud_options: Option<Vec<(String, String)>>,
        credential_provider: Option<Value>,
        retries: usize,
        sink_options: Wrap<SinkOptions>,
        metadata: Wrap<Option<KeyValueMetadata>>,
        field_overwrites: Vec<Wrap<ParquetFieldOverwrites>>,
    ) -> RbResult<RbLazyFrame> {
        let compression = parse_parquet_compression(&compression, compression_level)?;

        let options = ParquetWriteOptions {
            compression,
            statistics: statistics.0,
            row_group_size,
            data_page_size,
            key_value_metadata: metadata.0,
            field_overwrites: field_overwrites.into_iter().map(|f| f.0).collect(),
        };

        let cloud_options = match target.base_path() {
            None => None,
            Some(base_path) => {
                let cloud_options =
                    parse_cloud_options(base_path.to_str(), cloud_options.unwrap_or_default())?;
                Some(
                    cloud_options
                        .with_max_retries(retries)
                        .with_credential_provider(credential_provider.map(|_| todo!())),
                )
            }
        };

        let ldf = self.ldf.borrow().clone();
        match target {
            SinkTarget::File(target) => {
                ldf.sink_parquet(target, options, cloud_options, sink_options.0)
            }
        }
        .map_err(RbPolarsErr::from)
        .map(Into::into)
        .map_err(Into::into)
    }

    pub fn sink_ipc(
        &self,
        target: SinkTarget,
        compression: Wrap<Option<IpcCompression>>,
        compat_level: RbCompatLevel,
        cloud_options: Option<Vec<(String, String)>>,
        retries: usize,
        sink_options: Wrap<SinkOptions>,
    ) -> RbResult<RbLazyFrame> {
        let options = IpcWriterOptions {
            compression: compression.0,
            compat_level: compat_level.0,
            ..Default::default()
        };

        let cloud_options = match target.base_path() {
            None => None,
            Some(base_path) => {
                let cloud_options =
                    parse_cloud_options(base_path.to_str(), cloud_options.unwrap_or_default())?;
                Some(cloud_options.with_max_retries(retries))
            }
        };

        let ldf = self.ldf.borrow().clone();
        match target {
            SinkTarget::File(target) => {
                ldf.sink_ipc(target, options, cloud_options, sink_options.0)
            }
        }
        .map_err(RbPolarsErr::from)
        .map(Into::into)
        .map_err(Into::into)
    }

    pub fn sink_csv(&self, arguments: &[Value]) -> RbResult<RbLazyFrame> {
        let target = SinkTarget::try_convert(arguments[0])?;
        let include_bom = bool::try_convert(arguments[1])?;
        let include_header = bool::try_convert(arguments[2])?;
        let separator = u8::try_convert(arguments[3])?;
        let line_terminator = String::try_convert(arguments[4])?;
        let quote_char = u8::try_convert(arguments[5])?;
        let batch_size = NonZeroUsize::try_convert(arguments[6])?;
        let datetime_format = Option::<String>::try_convert(arguments[7])?;
        let date_format = Option::<String>::try_convert(arguments[8])?;
        let time_format = Option::<String>::try_convert(arguments[9])?;
        let float_scientific = Option::<bool>::try_convert(arguments[10])?;
        let float_precision = Option::<usize>::try_convert(arguments[11])?;
        let decimal_comma = bool::try_convert(arguments[12])?;
        let null_value = Option::<String>::try_convert(arguments[13])?;
        let quote_style = Option::<Wrap<QuoteStyle>>::try_convert(arguments[14])?;
        let cloud_options = Option::<Vec<(String, String)>>::try_convert(arguments[15])?;
        let retries = usize::try_convert(arguments[16])?;
        let sink_options = Wrap::<SinkOptions>::try_convert(arguments[17])?;

        let quote_style = quote_style.map_or(QuoteStyle::default(), |wrap| wrap.0);
        let null_value = null_value.unwrap_or(SerializeOptions::default().null);

        let serialize_options = SerializeOptions {
            date_format,
            time_format,
            datetime_format,
            float_scientific,
            float_precision,
            decimal_comma,
            separator,
            quote_char,
            null: null_value,
            line_terminator,
            quote_style,
        };

        let options = CsvWriterOptions {
            include_bom,
            include_header,
            batch_size,
            serialize_options,
        };

        let cloud_options = match target.base_path() {
            None => None,
            Some(base_path) => {
                let cloud_options =
                    parse_cloud_options(base_path.to_str(), cloud_options.unwrap_or_default())?;
                Some(cloud_options.with_max_retries(retries))
            }
        };

        let ldf = self.ldf.borrow().clone();
        match target {
            SinkTarget::File(target) => {
                ldf.sink_csv(target, options, cloud_options, sink_options.0)
            }
        }
        .map_err(RbPolarsErr::from)
        .map(Into::into)
        .map_err(Into::into)
    }

    pub fn sink_json(
        &self,
        target: SinkTarget,
        cloud_options: Option<Vec<(String, String)>>,
        retries: usize,
        sink_options: Wrap<SinkOptions>,
    ) -> RbResult<RbLazyFrame> {
        let options = JsonWriterOptions {};

        let cloud_options = match target.base_path() {
            None => None,
            Some(base_path) => {
                let cloud_options =
                    parse_cloud_options(base_path.to_str(), cloud_options.unwrap_or_default())?;
                Some(cloud_options.with_max_retries(retries))
            }
        };

        let ldf = self.ldf.borrow().clone();
        match target {
            SinkTarget::File(path) => ldf.sink_json(path, options, cloud_options, sink_options.0),
        }
        .map_err(RbPolarsErr::from)
        .map(Into::into)
        .map_err(Into::into)
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.filter(predicate.inner.clone()).into()
    }

    pub fn remove(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.remove(predicate.inner.clone()).into()
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
        allow_eq: bool,
        check_sortedness: bool,
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
            .how(JoinType::AsOf(Box::new(AsOfOptions {
                strategy: strategy.0,
                left_by: left_by.map(strings_to_pl_smallstr),
                right_by: right_by.map(strings_to_pl_smallstr),
                tolerance: tolerance.map(|t| {
                    let av = t.0.into_static();
                    let dtype = av.dtype();
                    Scalar::new(dtype, av)
                }),
                tolerance_str: tolerance_str.map(|s| s.into()),
                allow_eq,
                check_sortedness,
            })))
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
        maintain_order: Wrap<MaintainOrderJoin>,
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
            .maintain_order(maintain_order.0)
            .suffix(suffix)
            .finish()
            .into())
    }

    pub fn join_where(&self, other: &Self, predicates: RArray, suffix: String) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let other = other.ldf.borrow().clone();

        let predicates = rb_exprs_to_exprs(predicates)?;

        Ok(ldf
            .join_builder()
            .with(other)
            .suffix(suffix)
            .join_where(predicates)
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

    pub fn explode(&self, subset: &RbSelector) -> Self {
        self.ldf
            .borrow()
            .clone()
            .explode(subset.inner.clone())
            .into()
    }

    pub fn null_count(&self) -> Self {
        let ldf = self.ldf.borrow().clone();
        ldf.null_count().into()
    }

    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<&RbSelector>,
        keep: Wrap<UniqueKeepStrategy>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.borrow().clone();
        let subset = subset.map(|e| e.inner.clone());
        Ok(match maintain_order {
            true => ldf.unique_stable_generic(subset, keep.0),
            false => ldf.unique_generic(subset, keep.0),
        }
        .into())
    }

    pub fn drop_nans(&self, subset: Option<&RbSelector>) -> Self {
        self.ldf
            .borrow()
            .clone()
            .drop_nans(subset.map(|e| e.inner.clone()))
            .into()
    }

    pub fn drop_nulls(&self, subset: Option<&RbSelector>) -> Self {
        self.ldf
            .borrow()
            .clone()
            .drop_nulls(subset.map(|e| e.inner.clone()))
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
        on: &RbSelector,
        index: &RbSelector,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> RbResult<Self> {
        let args = UnpivotArgsDSL {
            on: on.inner.clone(),
            index: index.inner.clone(),
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

    pub fn drop(&self, columns: &RbSelector) -> Self {
        self.ldf.borrow().clone().drop(columns.inner.clone()).into()
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

    pub fn collect_schema(ruby: &Ruby, rb_self: &Self) -> RbResult<RHash> {
        let schema = rb_self
            .ldf
            .borrow_mut()
            .collect_schema()
            .map_err(RbPolarsErr::from)?;

        let schema_dict = ruby.hash_new();
        schema.iter_fields().for_each(|fld| {
            schema_dict
                .aset::<String, Value>(
                    fld.name().to_string(),
                    Wrap(fld.dtype().clone()).into_value_with(ruby),
                )
                .unwrap();
        });
        Ok(schema_dict)
    }

    pub fn unnest(&self, columns: &RbSelector, separator: Option<String>) -> Self {
        self.ldf
            .borrow()
            .clone()
            .unnest(
                columns.inner.clone(),
                separator.as_deref().map(PlSmallStr::from_str),
            )
            .into()
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

impl TryConvert for Wrap<polars_io::parquet::write::ParquetFieldOverwrites> {
    fn try_convert(_ob: Value) -> RbResult<Self> {
        todo!();
    }
}

unsafe impl TryConvertOwned for Wrap<polars_io::parquet::write::ParquetFieldOverwrites> {}

use magnus::{
    IntoValue, RArray, RHash, Ruby, TryConvert, Value, r_hash::ForEach, value::ReprValue,
};
use parking_lot::Mutex;
use polars::io::RowIndex;
use polars::lazy::frame::LazyFrame;
use polars::prelude::*;
use polars_plan::dsl::ScanSources;
use polars_plan::plans::{HintIR, Sorted};
use std::num::NonZeroUsize;

use super::{RbLazyFrame, RbOptFlags};
use crate::conversion::*;
use crate::expr::ToExprs;
use crate::expr::selector::RbSelector;
use crate::io::cloud_options::OptRbCloudOptions;
use crate::io::scan_options::RbScanOptions;
use crate::io::sink_options::RbSinkOptions;
use crate::io::sink_output::RbFileSinkDestination;
use crate::utils::{EnterPolarsExt, RubyAttach};
use crate::{RbDataFrame, RbExpr, RbLazyGroupBy, RbPolarsErr, RbResult, RbValueError};

fn rbobject_to_first_path_and_scan_sources(
    obj: Value,
) -> RbResult<(Option<PlRefPath>, ScanSources)> {
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
        let cloud_options = OptRbCloudOptions::try_convert(arguments[12])?;
        let credential_provider = Option::<Value>::try_convert(arguments[13])?;

        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });

        let sources = sources.0;
        let (first_path, sources) = match source {
            None => (sources.first_path().cloned(), sources),
            Some(source) => rbobject_to_first_path_and_scan_sources(source)?,
        };

        let mut r = LazyJsonLineReader::new_with_sources(sources);

        if let Some(first_path) = first_path {
            let first_path_url = first_path.as_str();

            let cloud_options = cloud_options.extract_opt_cloud_options(
                CloudScheme::from_path(first_path_url),
                credential_provider,
            )?;

            r = r.with_cloud_options(cloud_options);
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
        let cloud_options = OptRbCloudOptions::try_convert(arguments[28])?;
        let credential_provider = Option::<Value>::try_convert(arguments[29])?;
        let include_file_paths = Option::<String>::try_convert(arguments[30])?;
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
            None => (sources.first_path().cloned(), sources),
            Some(source) => rbobject_to_first_path_and_scan_sources(source)?,
        };

        let mut r = LazyCsvReader::new_with_sources(sources);

        if let Some(first_path) = first_path {
            let first_path_url = first_path.as_str();
            let cloud_options = cloud_options.extract_opt_cloud_options(
                CloudScheme::from_path(first_path_url),
                credential_provider,
            )?;
            r = r.with_cloud_options(cloud_options);
        }

        let mut r = r
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

        if let Some(lambda) = with_schema_modify {
            let f = |schema: Schema| {
                let iter = schema.iter_names().map(|s| s.as_str());
                Ruby::attach(|rb| {
                    let names = rb.ary_from_iter(iter);

                    let out = lambda
                        .funcall("call", (names,))
                        .expect("ruby function failed");
                    let new_names = Vec::<String>::try_convert(out)
                        .expect("ruby function should return Array[String]");
                    polars_ensure!(new_names.len() == schema.len(),
                        ShapeMismatch: "The length of the new names list should be equal to or less than the original column length",
                    );
                    Ok(schema
                        .iter_values()
                        .zip(new_names)
                        .map(|(dtype, name)| Field::new(name.into(), dtype.clone()))
                        .collect())
                })
            };
            r = r.with_schema_modify(f).map_err(RbPolarsErr::from)?
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
        let first_path = sources.first_path();

        let unified_scan_args =
            scan_options.extract_unified_scan_args(first_path.and_then(|x| x.scheme()))?;

        let lf: LazyFrame = DslBuilder::scan_parquet(sources, options, unified_scan_args)
            .map_err(to_rb_err)?
            .build()
            .into();

        Ok(lf.into())
    }

    pub fn new_from_ipc(
        sources: Wrap<ScanSources>,
        record_batch_statistics: bool,
        scan_options: RbScanOptions,
    ) -> RbResult<Self> {
        let options = IpcScanOptions {
            record_batch_statistics,
            checked: Default::default(),
        };

        let sources = sources.0;
        let first_path = sources.first_path().cloned();

        let unified_scan_args =
            scan_options.extract_unified_scan_args(first_path.as_ref().and_then(|x| x.scheme()))?;

        let lf = LazyFrame::scan_ipc_sources(sources, options, unified_scan_args)
            .map_err(RbPolarsErr::from)?;
        Ok(lf.into())
    }

    pub fn describe_plan(rb: &Ruby, self_: &Self) -> RbResult<String> {
        rb.enter_polars(|| self_.ldf.read().describe_plan())
    }

    pub fn describe_optimized_plan(rb: &Ruby, self_: &Self) -> RbResult<String> {
        rb.enter_polars(|| self_.ldf.read().describe_optimized_plan())
    }

    pub fn describe_plan_tree(rb: &Ruby, self_: &Self) -> RbResult<String> {
        rb.enter_polars(|| self_.ldf.read().describe_plan_tree())
    }

    pub fn describe_optimized_plan_tree(rb: &Ruby, self_: &Self) -> RbResult<String> {
        rb.enter_polars(|| self_.ldf.read().describe_optimized_plan_tree())
    }

    pub fn to_dot(rb: &Ruby, self_: &Self, optimized: bool) -> RbResult<String> {
        rb.enter_polars(|| self_.ldf.read().to_dot(optimized))
    }

    pub fn to_dot_streaming_phys(rb: &Ruby, self_: &Self, optimized: bool) -> RbResult<String> {
        rb.enter_polars(|| self_.ldf.read().to_dot_streaming_phys(optimized))
    }

    pub fn sort(
        &self,
        by_column: String,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
        multithreaded: bool,
    ) -> Self {
        let ldf = self.ldf.read().clone();
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
        let ldf = self.ldf.read().clone();
        let exprs = by.to_exprs()?;
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
        let ldf = self.ldf.read().clone();
        let exprs = by.to_exprs()?;
        Ok(ldf
            .top_k(
                k,
                exprs,
                SortMultipleOptions::new().with_order_descending_multi(reverse),
            )
            .into())
    }

    pub fn bottom_k(&self, k: IdxSize, by: RArray, reverse: Vec<bool>) -> RbResult<Self> {
        let ldf = self.ldf.read().clone();
        let exprs = by.to_exprs()?;
        Ok(ldf
            .bottom_k(
                k,
                exprs,
                SortMultipleOptions::new().with_order_descending_multi(reverse),
            )
            .into())
    }

    pub fn cache(&self) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.cache().into()
    }

    pub fn with_optimizations(&self, optflags: &RbOptFlags) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.with_optimizations(optflags.clone().inner.into_inner())
            .into()
    }

    pub fn profile(rb: &Ruby, self_: &Self) -> RbResult<(RbDataFrame, RbDataFrame)> {
        let (df, time_df) = rb.enter_polars(|| {
            let ldf = self_.ldf.read().clone();
            ldf.profile()
        })?;
        Ok((df.into(), time_df.into()))
    }

    pub fn collect(rb: &Ruby, self_: &Self, engine: Wrap<Engine>) -> RbResult<RbDataFrame> {
        rb.enter_polars_df(|| {
            let ldf = self_.ldf.read().clone();
            ldf.collect_with_engine(engine.0)
        })
    }

    pub fn collect_batches(
        rb: &Ruby,
        self_: &Self,
        engine: Wrap<Engine>,
        maintain_order: bool,
        chunk_size: Option<NonZeroUsize>,
        lazy: bool,
    ) -> RbResult<RbCollectBatches> {
        rb.enter_polars(|| {
            let ldf = self_.ldf.read().clone();

            let collect_batches = ldf
                .clone()
                .collect_batches(engine.0, maintain_order, chunk_size, lazy)
                .map_err(RbPolarsErr::from)?;

            RbResult::Ok(RbCollectBatches {
                inner: Arc::new(Mutex::new(collect_batches)),
                _ldf: ldf,
            })
        })
    }

    pub fn sink_parquet(
        rb: &Ruby,
        self_: &Self,
        target: RbFileSinkDestination,
        sink_options: RbSinkOptions,
        compression: String,
        compression_level: Option<i32>,
        statistics: Wrap<StatisticsOptions>,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
        metadata: Wrap<Option<KeyValueMetadata>>,
        arrow_schema: Option<Wrap<ArrowSchema>>,
    ) -> RbResult<RbLazyFrame> {
        let compression = parse_parquet_compression(&compression, compression_level)?;

        let options = ParquetWriteOptions {
            compression,
            statistics: statistics.0,
            row_group_size,
            data_page_size,
            key_value_metadata: metadata.0,
            arrow_schema: arrow_schema.map(|x| Arc::new(x.0)),
            compat_level: None,
        };

        let target = target.extract_file_sink_destination()?;
        let unified_sink_args = sink_options.extract_unified_sink_args(target.cloud_scheme())?;

        rb.enter_polars(|| {
            self_.ldf.read().clone().sink(
                target,
                FileWriteFormat::Parquet(Arc::new(options)),
                unified_sink_args,
            )
        })
        .map(Into::into)
    }

    pub fn sink_ipc(
        rb: &Ruby,
        self_: &Self,
        target: RbFileSinkDestination,
        sink_options: RbSinkOptions,
        compression: Wrap<Option<IpcCompression>>,
        compat_level: RbCompatLevel,
        record_batch_size: Option<usize>,
        record_batch_statistics: bool,
    ) -> RbResult<RbLazyFrame> {
        let options = IpcWriterOptions {
            compression: compression.0,
            compat_level: compat_level.0,
            record_batch_size,
            record_batch_statistics,
        };

        let target = target.extract_file_sink_destination()?;
        let unified_sink_args = sink_options.extract_unified_sink_args(target.cloud_scheme())?;

        rb.enter_polars(|| {
            self_
                .ldf
                .read()
                .clone()
                .sink(target, FileWriteFormat::Ipc(options), unified_sink_args)
        })
        .map(Into::into)
    }

    pub fn sink_csv(rb: &Ruby, self_: &Self, arguments: &[Value]) -> RbResult<RbLazyFrame> {
        let target = RbFileSinkDestination::try_convert(arguments[0])?;
        let sink_options = RbSinkOptions::try_convert(arguments[1])?;
        let include_bom = bool::try_convert(arguments[2])?;
        let compression = String::try_convert(arguments[3])?;
        let compression_level = Option::<u32>::try_convert(arguments[4])?;
        let check_extension = bool::try_convert(arguments[5])?;
        let include_header = bool::try_convert(arguments[6])?;
        let separator = u8::try_convert(arguments[7])?;
        let line_terminator = Wrap::<PlSmallStr>::try_convert(arguments[8])?;
        let quote_char = u8::try_convert(arguments[9])?;
        let batch_size = NonZeroUsize::try_convert(arguments[10])?;
        let datetime_format = Option::<Wrap<PlSmallStr>>::try_convert(arguments[11])?;
        let date_format = Option::<Wrap<PlSmallStr>>::try_convert(arguments[12])?;
        let time_format = Option::<Wrap<PlSmallStr>>::try_convert(arguments[13])?;
        let float_scientific = Option::<bool>::try_convert(arguments[14])?;
        let float_precision = Option::<usize>::try_convert(arguments[15])?;
        let decimal_comma = bool::try_convert(arguments[16])?;
        let null_value = Option::<Wrap<PlSmallStr>>::try_convert(arguments[17])?;
        let quote_style = Option::<Wrap<QuoteStyle>>::try_convert(arguments[18])?;

        let quote_style = quote_style.map_or(QuoteStyle::default(), |wrap| wrap.0);
        let null_value = null_value
            .map(|x| x.0)
            .unwrap_or(SerializeOptions::default().null);

        let serialize_options = SerializeOptions {
            date_format: date_format.map(|x| x.0),
            time_format: time_format.map(|x| x.0),
            datetime_format: datetime_format.map(|x| x.0),
            float_scientific,
            float_precision,
            decimal_comma,
            separator,
            quote_char,
            null: null_value,
            line_terminator: line_terminator.0,
            quote_style,
        };

        let options = CsvWriterOptions {
            include_bom,
            compression: ExternalCompression::try_from(&compression, compression_level)
                .map_err(RbPolarsErr::from)?,
            check_extension,
            include_header,
            batch_size,
            serialize_options: serialize_options.into(),
        };

        let target = target.extract_file_sink_destination()?;
        let unified_sink_args = sink_options.extract_unified_sink_args(target.cloud_scheme())?;

        rb.enter_polars(|| {
            self_
                .ldf
                .read()
                .clone()
                .sink(target, FileWriteFormat::Csv(options), unified_sink_args)
        })
        .map(Into::into)
    }

    pub fn sink_ndjson(
        rb: &Ruby,
        self_: &Self,
        target: RbFileSinkDestination,
        compression: String,
        compression_level: Option<u32>,
        check_extension: bool,
        sink_options: RbSinkOptions,
    ) -> RbResult<RbLazyFrame> {
        let options = NDJsonWriterOptions {
            compression: ExternalCompression::try_from(&compression, compression_level)
                .map_err(RbPolarsErr::from)?,
            check_extension,
        };

        let target = target.extract_file_sink_destination()?;
        let unified_sink_args = sink_options.extract_unified_sink_args(target.cloud_scheme())?;

        rb.enter_polars(|| {
            self_.ldf.read().clone().sink(
                target,
                FileWriteFormat::NDJson(options),
                unified_sink_args,
            )
        })
        .map(Into::into)
    }

    pub fn filter(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.filter(predicate.inner.clone()).into()
    }

    pub fn remove(&self, predicate: &RbExpr) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.remove(predicate.inner.clone()).into()
    }

    pub fn select(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.read().clone();
        let exprs = exprs.to_exprs()?;
        Ok(ldf.select(exprs).into())
    }

    pub fn select_seq(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.read().clone();
        let exprs = exprs.to_exprs()?;
        Ok(ldf.select_seq(exprs).into())
    }

    pub fn group_by(&self, by: RArray, maintain_order: bool) -> RbResult<RbLazyGroupBy> {
        let ldf = self.ldf.read().clone();
        let by = by.to_exprs()?;
        let lazy_gb = if maintain_order {
            ldf.group_by_stable(by)
        } else {
            ldf.group_by(by)
        };
        Ok(RbLazyGroupBy { lgb: Some(lazy_gb) })
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
        let ldf = self.ldf.read().clone();
        let by = by.to_exprs()?;
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

        Ok(RbLazyGroupBy { lgb: Some(lazy_gb) })
    }

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
        let by = by.to_exprs()?;
        let ldf = self.ldf.read().clone();
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

        Ok(RbLazyGroupBy { lgb: Some(lazy_gb) })
    }

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
        let ldf = self.ldf.read().clone();
        let other = other.ldf.read().clone();
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
        let ldf = self.ldf.read().clone();
        let other = other.ldf.read().clone();
        let left_on = left_on.to_exprs()?;
        let right_on = right_on.to_exprs()?;

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
        let ldf = self.ldf.read().clone();
        let other = other.ldf.read().clone();

        let predicates = predicates.to_exprs()?;

        Ok(ldf
            .join_builder()
            .with(other)
            .suffix(suffix)
            .join_where(predicates)
            .into())
    }

    pub fn with_column(&self, expr: &RbExpr) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.with_column(expr.inner.clone()).into()
    }

    pub fn with_columns(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.read().clone();
        Ok(ldf.with_columns(exprs.to_exprs()?).into())
    }

    pub fn with_columns_seq(&self, exprs: RArray) -> RbResult<Self> {
        let ldf = self.ldf.read().clone();
        Ok(ldf.with_columns_seq(exprs.to_exprs()?).into())
    }

    pub fn rename(&self, existing: Vec<String>, new: Vec<String>, strict: bool) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.rename(existing, new, strict).into()
    }

    pub fn reverse(&self) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.reverse().into()
    }

    pub fn shift(&self, n: &RbExpr, fill_value: Option<&RbExpr>) -> Self {
        let lf = self.ldf.read().clone();
        let out = match fill_value {
            Some(v) => lf.shift_and_fill(n.inner.clone(), v.inner.clone()),
            None => lf.shift(n.inner.clone()),
        };
        out.into()
    }

    pub fn fill_nan(&self, fill_value: &RbExpr) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.fill_nan(fill_value.inner.clone()).into()
    }

    pub fn min(&self) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.min();
        out.into()
    }

    pub fn max(&self) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.max();
        out.into()
    }

    pub fn sum(&self) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.sum();
        out.into()
    }

    pub fn mean(&self) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.mean();
        out.into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.std(ddof);
        out.into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.var(ddof);
        out.into()
    }

    pub fn median(&self) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.median();
        out.into()
    }

    pub fn quantile(&self, quantile: &RbExpr, interpolation: Wrap<QuantileMethod>) -> Self {
        let ldf = self.ldf.read().clone();
        let out = ldf.quantile(quantile.inner.clone(), interpolation.0);
        out.into()
    }

    pub fn explode(&self, subset: &RbSelector, empty_as_null: bool, keep_nulls: bool) -> Self {
        self.ldf
            .read()
            .clone()
            .explode(
                subset.inner.clone(),
                ExplodeOptions {
                    empty_as_null,
                    keep_nulls,
                },
            )
            .into()
    }

    pub fn null_count(&self) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.null_count().into()
    }

    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<RArray>,
        keep: Wrap<UniqueKeepStrategy>,
    ) -> RbResult<Self> {
        let ldf = self.ldf.read().clone();
        let subset = match subset {
            Some(e) => Some(e.to_exprs()?),
            None => None,
        };
        Ok(match maintain_order {
            true => ldf.unique_stable_generic(subset, keep.0),
            false => ldf.unique_generic(subset, keep.0),
        }
        .into())
    }

    pub fn drop_nans(&self, subset: Option<&RbSelector>) -> Self {
        self.ldf
            .read()
            .clone()
            .drop_nans(subset.map(|e| e.inner.clone()))
            .into()
    }

    pub fn drop_nulls(&self, subset: Option<&RbSelector>) -> Self {
        self.ldf
            .read()
            .clone()
            .drop_nulls(subset.map(|e| e.inner.clone()))
            .into()
    }

    pub fn slice(&self, offset: i64, len: Option<IdxSize>) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.slice(offset, len.unwrap_or(IdxSize::MAX)).into()
    }

    pub fn tail(&self, n: IdxSize) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.tail(n).into()
    }

    pub fn pivot(
        &self,
        on: &RbSelector,
        on_columns: &RbDataFrame,
        index: &RbSelector,
        values: &RbSelector,
        agg: &RbExpr,
        maintain_order: bool,
        separator: String,
    ) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.pivot(
            on.inner.clone(),
            Arc::new(on_columns.df.read().clone()),
            index.inner.clone(),
            values.inner.clone(),
            agg.inner.clone(),
            maintain_order,
            separator.into(),
        )
        .into()
    }

    pub fn unpivot(
        &self,
        on: Option<&RbSelector>,
        index: &RbSelector,
        value_name: Option<String>,
        variable_name: Option<String>,
    ) -> RbResult<Self> {
        let args = UnpivotArgsDSL {
            on: on.map(|on| on.inner.clone()),
            index: index.inner.clone(),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
        };

        let ldf = self.ldf.read().clone();
        Ok(ldf.unpivot(args).into())
    }

    pub fn with_row_index(&self, name: String, offset: Option<IdxSize>) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.with_row_index(&name, offset).into()
    }

    pub fn drop(&self, columns: &RbSelector) -> Self {
        self.ldf.read().clone().drop(columns.inner.clone()).into()
    }

    pub fn cast(&self, rb_dtypes: RHash, strict: bool) -> RbResult<Self> {
        let mut dtypes = Vec::new();
        rb_dtypes.foreach(|k: String, v: Wrap<DataType>| {
            dtypes.push((k, v.0));
            Ok(ForEach::Continue)
        })?;
        let mut cast_map = PlHashMap::with_capacity(dtypes.len());
        cast_map.extend(dtypes.iter().map(|(k, v)| (k.as_ref(), v.clone())));
        Ok(self.ldf.read().clone().cast(cast_map, strict).into())
    }

    pub fn cast_all(&self, dtype: Wrap<DataType>, strict: bool) -> Self {
        self.ldf.read().clone().cast_all(dtype.0, strict).into()
    }

    pub fn clone(&self) -> Self {
        self.ldf.read().clone().into()
    }

    pub fn collect_schema(rb: &Ruby, self_: &Self) -> RbResult<RHash> {
        let schema = rb.enter_polars(|| self_.ldf.write().collect_schema())?;

        let schema_dict = rb.hash_new();
        schema.iter_fields().for_each(|fld| {
            schema_dict
                .aset::<String, Value>(
                    fld.name().to_string(),
                    Wrap(fld.dtype().clone()).into_value_with(rb),
                )
                .unwrap();
        });
        Ok(schema_dict)
    }

    pub fn unnest(&self, columns: &RbSelector, separator: Option<String>) -> Self {
        self.ldf
            .read()
            .clone()
            .unnest(
                columns.inner.clone(),
                separator.as_deref().map(PlSmallStr::from_str),
            )
            .into()
    }

    pub fn count(&self) -> Self {
        let ldf = self.ldf.read().clone();
        ldf.count().into()
    }

    pub fn merge_sorted(&self, other: &Self, key: String) -> RbResult<Self> {
        let out = self
            .ldf
            .read()
            .clone()
            .merge_sorted(other.ldf.read().clone(), &key)
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn hint_sorted(
        &self,
        columns: Vec<String>,
        descending: Vec<bool>,
        nulls_last: Vec<bool>,
    ) -> RbResult<Self> {
        if columns.len() != descending.len() && descending.len() != 1 {
            return Err(RbValueError::new_err(
                "`set_sorted` expects the same amount of `columns` as `descending` values.",
            ));
        }
        if columns.len() != nulls_last.len() && nulls_last.len() != 1 {
            return Err(RbValueError::new_err(
                "`set_sorted` expects the same amount of `columns` as `nulls_last` values.",
            ));
        }

        let mut sorted = columns
            .iter()
            .map(|c| Sorted {
                column: PlSmallStr::from_str(c.as_str()),
                descending: Some(false),
                nulls_last: Some(false),
            })
            .collect::<Vec<_>>();

        if !columns.is_empty() {
            if descending.len() != 1 {
                sorted
                    .iter_mut()
                    .zip(descending)
                    .for_each(|(s, d)| s.descending = Some(d));
            } else if descending[0] {
                sorted.iter_mut().for_each(|s| s.descending = Some(true));
            }

            if nulls_last.len() != 1 {
                sorted
                    .iter_mut()
                    .zip(nulls_last)
                    .for_each(|(s, d)| s.nulls_last = Some(d));
            } else if nulls_last[0] {
                sorted.iter_mut().for_each(|s| s.nulls_last = Some(true));
            }
        }

        let out = self
            .ldf
            .read()
            .clone()
            .hint(HintIR::Sorted(sorted.into()))
            .map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }
}

#[magnus::wrap(class = "Polars::RbCollectBatches")]
pub struct RbCollectBatches {
    inner: Arc<Mutex<CollectBatches>>,
    _ldf: LazyFrame,
}

impl RbCollectBatches {
    pub fn next(rb: &Ruby, slf: &Self) -> RbResult<Option<RbDataFrame>> {
        let inner = Arc::clone(&slf.inner);
        rb.enter_polars(|| PolarsResult::Ok(inner.lock().next().transpose()?.map(RbDataFrame::new)))
    }
}

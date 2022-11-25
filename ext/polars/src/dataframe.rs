use magnus::{r_hash::ForEach, Error, RArray, RHash, RString, Value};
use polars::io::mmap::ReaderBytes;
use polars::io::RowCount;
use polars::prelude::*;
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor};
use std::ops::Deref;
use std::path::PathBuf;

use crate::conversion::*;
use crate::file::{get_file_like, get_mmap_bytes_reader};
use crate::series::to_rbseries_collection;
use crate::{series, RbLazyFrame, RbPolarsErr, RbResult, RbSeries};

#[magnus::wrap(class = "Polars::RbDataFrame")]
pub struct RbDataFrame {
    pub df: RefCell<DataFrame>,
}

impl From<DataFrame> for RbDataFrame {
    fn from(df: DataFrame) -> Self {
        RbDataFrame::new(df)
    }
}

impl RbDataFrame {
    pub fn new(df: DataFrame) -> Self {
        RbDataFrame {
            df: RefCell::new(df),
        }
    }

    pub fn init(columns: RArray) -> RbResult<Self> {
        let mut cols = Vec::new();
        for i in columns.each() {
            cols.push(i?.try_convert::<&RbSeries>()?.series.borrow().clone());
        }
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn estimated_size(&self) -> usize {
        self.df.borrow().estimated_size()
    }

    pub fn read_csv(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let rb_f: Value = arguments[0].try_convert()?;
        let infer_schema_length: Option<usize> = arguments[1].try_convert()?;
        let chunk_size: usize = arguments[2].try_convert()?;
        let has_header: bool = arguments[3].try_convert()?;
        let ignore_errors: bool = arguments[4].try_convert()?;
        let n_rows: Option<usize> = arguments[5].try_convert()?;
        let skip_rows: usize = arguments[6].try_convert()?;
        let projection: Option<Vec<usize>> = arguments[7].try_convert()?;
        let sep: String = arguments[8].try_convert()?;
        let rechunk: bool = arguments[9].try_convert()?;
        let columns: Option<Vec<String>> = arguments[10].try_convert()?;
        let encoding: Wrap<CsvEncoding> = arguments[11].try_convert()?;
        let n_threads: Option<usize> = arguments[12].try_convert()?;
        let path: Option<String> = arguments[13].try_convert()?;
        let overwrite_dtype: Option<Vec<(String, Wrap<DataType>)>> = arguments[14].try_convert()?;
        // TODO fix
        let overwrite_dtype_slice: Option<Vec<Wrap<DataType>>> = None; // arguments[15].try_convert()?;
        let low_memory: bool = arguments[16].try_convert()?;
        let comment_char: Option<String> = arguments[17].try_convert()?;
        let quote_char: Option<String> = arguments[18].try_convert()?;
        let null_values: Option<Wrap<NullValues>> = arguments[19].try_convert()?;
        let parse_dates: bool = arguments[20].try_convert()?;
        let skip_rows_after_header: usize = arguments[21].try_convert()?;
        let row_count: Option<(String, IdxSize)> = arguments[22].try_convert()?;
        let sample_size: usize = arguments[23].try_convert()?;
        let eol_char: String = arguments[24].try_convert()?;
        // end arguments

        let null_values = null_values.map(|w| w.0);
        let comment_char = comment_char.map(|s| s.as_bytes()[0]);
        let eol_char = eol_char.as_bytes()[0];

        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });

        let quote_char = if let Some(s) = quote_char {
            if s.is_empty() {
                None
            } else {
                Some(s.as_bytes()[0])
            }
        } else {
            None
        };

        let overwrite_dtype = overwrite_dtype.map(|overwrite_dtype| {
            let fields = overwrite_dtype.iter().map(|(name, dtype)| {
                let dtype = dtype.0.clone();
                Field::new(name, dtype)
            });
            Schema::from(fields)
        });

        let overwrite_dtype_slice = overwrite_dtype_slice.map(|overwrite_dtype| {
            overwrite_dtype
                .iter()
                .map(|dt| dt.0.clone())
                .collect::<Vec<_>>()
        });

        let mmap_bytes_r = get_mmap_bytes_reader(rb_f)?;
        let df = CsvReader::new(mmap_bytes_r)
            .infer_schema(infer_schema_length)
            .has_header(has_header)
            .with_n_rows(n_rows)
            .with_delimiter(sep.as_bytes()[0])
            .with_skip_rows(skip_rows)
            .with_ignore_parser_errors(ignore_errors)
            .with_projection(projection)
            .with_rechunk(rechunk)
            .with_chunk_size(chunk_size)
            .with_encoding(encoding.0)
            .with_columns(columns)
            .with_n_threads(n_threads)
            .with_path(path)
            .with_dtypes(overwrite_dtype.as_ref())
            .with_dtypes_slice(overwrite_dtype_slice.as_deref())
            .low_memory(low_memory)
            .with_comment_char(comment_char)
            .with_null_values(null_values)
            .with_parse_dates(parse_dates)
            .with_quote_char(quote_char)
            .with_end_of_line_char(eol_char)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_row_count(row_count)
            .sample_size(sample_size)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn read_parquet(path: PathBuf) -> RbResult<Self> {
        let f = File::open(&path).map_err(|e| Error::runtime_error(e.to_string()))?;
        let reader = BufReader::new(f);
        ParquetReader::new(reader)
            .finish()
            .map_err(RbPolarsErr::from)
            .map(|v| v.into())
    }

    pub fn read_ipc(
        rb_f: Value,
        columns: Option<Vec<String>>,
        projection: Option<Vec<usize>>,
        n_rows: Option<usize>,
        row_count: Option<(String, IdxSize)>,
        memory_map: bool,
    ) -> RbResult<Self> {
        let row_count = row_count.map(|(name, offset)| RowCount { name, offset });
        let mmap_bytes_r = get_mmap_bytes_reader(rb_f)?;
        let df = IpcReader::new(mmap_bytes_r)
            .with_projection(projection)
            .with_columns(columns)
            .with_n_rows(n_rows)
            .with_row_count(row_count)
            .memory_mapped(memory_map)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn read_json(rb_f: Value) -> RbResult<Self> {
        // memmap the file first
        let mmap_bytes_r = get_mmap_bytes_reader(rb_f)?;
        let mmap_read: ReaderBytes = (&mmap_bytes_r).into();
        let bytes = mmap_read.deref();

        // Happy path is our column oriented json as that is most performant
        // on failure we try
        match serde_json::from_slice::<DataFrame>(bytes) {
            Ok(df) => Ok(df.into()),
            // try arrow json reader instead
            // this is row oriented
            Err(_) => {
                let out = JsonReader::new(mmap_bytes_r)
                    .with_json_format(JsonFormat::Json)
                    .finish()
                    .map_err(|e| RbPolarsErr::other(format!("{:?}", e)))?;
                Ok(out.into())
            }
        }
    }

    pub fn read_ndjson(rb_f: Value) -> RbResult<Self> {
        let mmap_bytes_r = get_mmap_bytes_reader(rb_f)?;

        let out = JsonReader::new(mmap_bytes_r)
            .with_json_format(JsonFormat::JsonLines)
            .finish()
            .map_err(|e| RbPolarsErr::other(format!("{:?}", e)))?;
        Ok(out.into())
    }

    pub fn write_json(&self, rb_f: Value, pretty: bool, row_oriented: bool) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);

        let r = match (pretty, row_oriented) {
            (_, true) => JsonWriter::new(file)
                .with_json_format(JsonFormat::Json)
                .finish(&mut self.df.borrow_mut()),
            (true, _) => serde_json::to_writer_pretty(file, &*self.df.borrow())
                .map_err(|e| PolarsError::ComputeError(format!("{:?}", e).into())),
            (false, _) => serde_json::to_writer(file, &*self.df.borrow())
                .map_err(|e| PolarsError::ComputeError(format!("{:?}", e).into())),
        };
        r.map_err(|e| RbPolarsErr::other(format!("{:?}", e)))?;
        Ok(())
    }

    pub fn write_ndjson(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);

        let r = JsonWriter::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut self.df.borrow_mut());

        r.map_err(|e| RbPolarsErr::other(format!("{:?}", e)))?;
        Ok(())
    }

    pub fn read_hash(data: RHash) -> RbResult<Self> {
        let mut cols: Vec<Series> = Vec::new();
        data.foreach(|name: String, values: Value| {
            let obj: Value = series().funcall("new", (name, values))?;
            let rbseries = obj.funcall::<_, _, &RbSeries>("_s", ())?;
            cols.push(rbseries.series.borrow().clone());
            Ok(ForEach::Continue)
        })?;
        let df = DataFrame::new(cols).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_csv(
        &self,
        rb_f: Value,
        has_header: bool,
        sep: u8,
        quote: u8,
        batch_size: usize,
        datetime_format: Option<String>,
        date_format: Option<String>,
        time_format: Option<String>,
        float_precision: Option<usize>,
        null_value: Option<String>,
    ) -> RbResult<()> {
        let null = null_value.unwrap_or_default();

        if let Ok(s) = rb_f.try_convert::<String>() {
            let f = std::fs::File::create(&s).unwrap();
            // no need for a buffered writer, because the csv writer does internal buffering
            CsvWriter::new(f)
                .has_header(has_header)
                .with_delimiter(sep)
                .with_quoting_char(quote)
                .with_batch_size(batch_size)
                .with_datetime_format(datetime_format)
                .with_date_format(date_format)
                .with_time_format(time_format)
                .with_float_precision(float_precision)
                .with_null_value(null)
                .finish(&mut self.df.borrow_mut())
                .map_err(RbPolarsErr::from)?;
        } else {
            let mut buf = Cursor::new(Vec::new());
            CsvWriter::new(&mut buf)
                .has_header(has_header)
                .with_delimiter(sep)
                .with_quoting_char(quote)
                .with_batch_size(batch_size)
                .with_datetime_format(datetime_format)
                .with_date_format(date_format)
                .with_time_format(time_format)
                .with_float_precision(float_precision)
                .with_null_value(null)
                .finish(&mut self.df.borrow_mut())
                .map_err(RbPolarsErr::from)?;
            // TODO less copying
            let rb_str = RString::from_slice(&buf.into_inner());
            rb_f.funcall::<_, _, Value>("write", (rb_str,))?;
        }

        Ok(())
    }

    pub fn write_ipc(
        &self,
        rb_f: Value,
        compression: Wrap<Option<IpcCompression>>,
    ) -> RbResult<()> {
        if let Ok(s) = rb_f.try_convert::<String>() {
            let f = std::fs::File::create(&s).unwrap();
            IpcWriter::new(f)
                .with_compression(compression.0)
                .finish(&mut self.df.borrow_mut())
                .map_err(RbPolarsErr::from)?;
        } else {
            let mut buf = get_file_like(rb_f, true)?;

            IpcWriter::new(&mut buf)
                .with_compression(compression.0)
                .finish(&mut self.df.borrow_mut())
                .map_err(RbPolarsErr::from)?;
        }
        Ok(())
    }

    pub fn write_parquet(
        &self,
        rb_f: Value,
        compression: String,
        compression_level: Option<i32>,
        statistics: bool,
        row_group_size: Option<usize>,
    ) -> RbResult<()> {
        let compression = parse_parquet_compression(&compression, compression_level)?;

        if let Ok(s) = rb_f.try_convert::<String>() {
            let f = std::fs::File::create(&s).unwrap();
            ParquetWriter::new(f)
                .with_compression(compression)
                .with_statistics(statistics)
                .with_row_group_size(row_group_size)
                .finish(&mut self.df.borrow_mut())
                .map_err(RbPolarsErr::from)?;
        } else {
            todo!();
        }

        Ok(())
    }

    pub fn rechunk(&self) -> Self {
        self.df.borrow().agg_chunks().into()
    }

    pub fn to_s(&self) -> String {
        format!("{}", self.df.borrow())
    }

    pub fn get_columns(&self) -> Vec<RbSeries> {
        let cols = self.df.borrow().get_columns().clone();
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

    pub fn dtypes(&self) -> Vec<Value> {
        self.df
            .borrow()
            .iter()
            .map(|s| Wrap(s.dtype().clone()).into())
            .collect()
    }

    pub fn n_chunks(&self) -> RbResult<usize> {
        let n = self.df.borrow().n_chunks().map_err(RbPolarsErr::from)?;
        Ok(n)
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

    pub fn select_at_idx(&self, idx: usize) -> Option<RbSeries> {
        self.df
            .borrow()
            .select_at_idx(idx)
            .map(|s| RbSeries::new(s.clone()))
    }

    // TODO remove clone
    pub fn column(&self, name: String) -> RbResult<RbSeries> {
        self.df
            .borrow()
            .column(&name)
            .map(|v| v.clone().into())
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

    pub fn sort(&self, by_column: String, reverse: bool, nulls_last: bool) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .sort_with_options(
                &by_column,
                SortOptions {
                    descending: reverse,
                    nulls_last,
                },
            )
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn replace(&self, column: String, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .borrow_mut()
            .replace(&column, new_col.series.borrow().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn replace_at_idx(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .borrow_mut()
            .replace_at_idx(index, new_col.series.borrow().clone())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn insert_at_idx(&self, index: usize, new_col: &RbSeries) -> RbResult<()> {
        self.df
            .borrow_mut()
            .insert_at_idx(index, new_col.series.borrow().clone())
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

    pub fn frame_equal(&self, other: &RbDataFrame, null_equal: bool) -> bool {
        if null_equal {
            self.df.borrow().frame_equal_missing(&other.df.borrow())
        } else {
            self.df.borrow().frame_equal(&other.df.borrow())
        }
    }

    pub fn with_row_count(&self, name: String, offset: Option<IdxSize>) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .with_row_count(&name, offset)
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
            id_vars,
            value_vars,
            value_name,
            variable_name,
        };

        let df = self.df.borrow().melt2(args).map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn partition_by(&self, groups: Vec<String>, stable: bool) -> RbResult<Vec<Self>> {
        let out = if stable {
            self.df.borrow().partition_by_stable(groups)
        } else {
            self.df.borrow().partition_by(groups)
        }
        .map_err(RbPolarsErr::from)?;
        Ok(out.into_iter().map(RbDataFrame::new).collect())
    }

    pub fn shift(&self, periods: i64) -> Self {
        self.df.borrow().shift(periods).into()
    }

    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<Vec<String>>,
        keep: Wrap<UniqueKeepStrategy>,
    ) -> RbResult<Self> {
        let subset = subset.as_ref().map(|v| v.as_ref());
        let df = match maintain_order {
            true => self.df.borrow().unique_stable(subset, keep.0),
            false => self.df.borrow().unique(subset, keep.0),
        }
        .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn lazy(&self) -> RbLazyFrame {
        self.df.borrow().clone().lazy().into()
    }

    pub fn max(&self) -> Self {
        self.df.borrow().max().into()
    }

    pub fn min(&self) -> Self {
        self.df.borrow().min().into()
    }

    pub fn sum(&self) -> Self {
        self.df.borrow().sum().into()
    }

    pub fn mean(&self) -> Self {
        self.df.borrow().mean().into()
    }

    pub fn std(&self, ddof: u8) -> Self {
        self.df.borrow().std(ddof).into()
    }

    pub fn var(&self, ddof: u8) -> Self {
        self.df.borrow().var(ddof).into()
    }

    pub fn median(&self) -> Self {
        self.df.borrow().median().into()
    }

    pub fn hmean(&self, null_strategy: Wrap<NullStrategy>) -> RbResult<Option<RbSeries>> {
        let s = self
            .df
            .borrow()
            .hmean(null_strategy.0)
            .map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn hmax(&self) -> RbResult<Option<RbSeries>> {
        let s = self.df.borrow().hmax().map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn hmin(&self) -> RbResult<Option<RbSeries>> {
        let s = self.df.borrow().hmin().map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn hsum(&self, null_strategy: Wrap<NullStrategy>) -> RbResult<Option<RbSeries>> {
        let s = self
            .df
            .borrow()
            .hsum(null_strategy.0)
            .map_err(RbPolarsErr::from)?;
        Ok(s.map(|s| s.into()))
    }

    pub fn quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> RbResult<Self> {
        let df = self
            .df
            .borrow()
            .quantile(quantile, interpolation.0)
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn to_dummies(&self, columns: Option<Vec<String>>) -> RbResult<Self> {
        let df = match columns {
            Some(cols) => self
                .df
                .borrow()
                .columns_to_dummies(cols.iter().map(|x| x as &str).collect()),
            None => self.df.borrow().to_dummies(),
        }
        .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn null_count(&self) -> Self {
        let df = self.df.borrow().null_count();
        df.into()
    }

    pub fn shrink_to_fit(&self) {
        self.df.borrow_mut().shrink_to_fit();
    }

    pub fn transpose(&self, include_header: bool, names: String) -> RbResult<Self> {
        let mut df = self.df.borrow().transpose().map_err(RbPolarsErr::from)?;
        if include_header {
            let s = Utf8Chunked::from_iter_values(
                &names,
                self.df.borrow().get_columns().iter().map(|s| s.name()),
            )
            .into_series();
            df.insert_at_idx(0, s).unwrap();
        }
        Ok(df.into())
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

    pub fn unnest(&self, names: Vec<String>) -> RbResult<Self> {
        let df = self.df.borrow().unnest(names).map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }
}

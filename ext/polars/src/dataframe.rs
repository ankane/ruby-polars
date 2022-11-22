use magnus::{r_hash::ForEach, Error, RArray, RHash, RString, Value};
use polars::io::mmap::ReaderBytes;
use polars::prelude::*;
use std::cell::RefCell;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor};
use std::ops::Deref;
use std::path::PathBuf;

use crate::conversion::parse_parquet_compression;
use crate::file::{get_file_like, get_mmap_bytes_reader};
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

    pub fn read_csv(rb_f: Value, has_header: bool) -> RbResult<Self> {
        let mmap_bytes_r = get_mmap_bytes_reader(rb_f)?;
        let df = CsvReader::new(mmap_bytes_r)
            .has_header(has_header)
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

    pub fn columns(&self) -> Vec<String> {
        self.df
            .borrow()
            .get_column_names()
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    pub fn dtypes(&self) -> Vec<String> {
        self.df
            .borrow()
            .iter()
            .map(|s| s.dtype().to_string())
            .collect()
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

    pub fn head(&self, length: Option<usize>) -> Self {
        self.df.borrow().head(length).into()
    }

    pub fn tail(&self, length: Option<usize>) -> Self {
        self.df.borrow().tail(length).into()
    }

    pub fn frame_equal(&self, other: &RbDataFrame, null_equal: bool) -> bool {
        if null_equal {
            self.df.borrow().frame_equal_missing(&other.df.borrow())
        } else {
            self.df.borrow().frame_equal(&other.df.borrow())
        }
    }

    pub fn lazy(&self) -> RbLazyFrame {
        self.df.borrow().clone().lazy().into()
    }

    pub fn mean(&self) -> Self {
        self.df.borrow().mean().into()
    }

    pub fn null_count(&self) -> Self {
        let df = self.df.borrow().null_count();
        df.into()
    }
}

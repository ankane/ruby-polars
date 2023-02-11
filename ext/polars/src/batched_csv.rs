use magnus::{RArray, Value};
use polars::io::mmap::MmapBytesReader;
use polars::io::RowCount;
use polars::prelude::read_impl::OwnedBatchedCsvReader;
use polars::prelude::*;
use std::cell::RefCell;
use std::path::PathBuf;

use crate::conversion::*;
use crate::{RbDataFrame, RbPolarsErr, RbResult};

#[magnus::wrap(class = "Polars::RbBatchedCsv")]
pub struct RbBatchedCsv {
    pub reader: RefCell<OwnedBatchedCsvReader>,
}

impl RbBatchedCsv {
    pub fn new(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let infer_schema_length: Option<usize> = arguments[0].try_convert()?;
        let chunk_size: usize = arguments[1].try_convert()?;
        let has_header: bool = arguments[2].try_convert()?;
        let ignore_errors: bool = arguments[3].try_convert()?;
        let n_rows: Option<usize> = arguments[4].try_convert()?;
        let skip_rows: usize = arguments[5].try_convert()?;
        let projection: Option<Vec<usize>> = arguments[6].try_convert()?;
        let sep: String = arguments[7].try_convert()?;
        let rechunk: bool = arguments[8].try_convert()?;
        let columns: Option<Vec<String>> = arguments[9].try_convert()?;
        let encoding: Wrap<CsvEncoding> = arguments[10].try_convert()?;
        let n_threads: Option<usize> = arguments[11].try_convert()?;
        let path: PathBuf = arguments[12].try_convert()?;
        let overwrite_dtype: Option<Vec<(String, Wrap<DataType>)>> = arguments[13].try_convert()?;
        // TODO fix
        let overwrite_dtype_slice: Option<Vec<Wrap<DataType>>> = None; // arguments[14].try_convert()?;
        let low_memory: bool = arguments[15].try_convert()?;
        let comment_char: Option<String> = arguments[16].try_convert()?;
        let quote_char: Option<String> = arguments[17].try_convert()?;
        let null_values: Option<Wrap<NullValues>> = arguments[18].try_convert()?;
        let parse_dates: bool = arguments[19].try_convert()?;
        let skip_rows_after_header: usize = arguments[20].try_convert()?;
        let row_count: Option<(String, IdxSize)> = arguments[21].try_convert()?;
        let sample_size: usize = arguments[22].try_convert()?;
        let eol_char: String = arguments[23].try_convert()?;
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

        let file = std::fs::File::open(path).map_err(RbPolarsErr::io)?;
        let reader = Box::new(file) as Box<dyn MmapBytesReader>;
        let reader = CsvReader::new(reader)
            .infer_schema(infer_schema_length)
            .has_header(has_header)
            .with_n_rows(n_rows)
            .with_delimiter(sep.as_bytes()[0])
            .with_skip_rows(skip_rows)
            .with_ignore_errors(ignore_errors)
            .with_projection(projection)
            .with_rechunk(rechunk)
            .with_chunk_size(chunk_size)
            .with_encoding(encoding.0)
            .with_columns(columns)
            .with_n_threads(n_threads)
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
            .batched(overwrite_dtype.map(Arc::new))
            .map_err(RbPolarsErr::from)?;

        Ok(RbBatchedCsv {
            reader: RefCell::new(reader),
        })
    }

    pub fn next_batches(&self, n: usize) -> RbResult<Option<RArray>> {
        let batches = self
            .reader
            .borrow_mut()
            .next_batches(n)
            .map_err(RbPolarsErr::from)?;
        Ok(batches.map(|batches| {
            RArray::from_iter(batches.into_iter().map(|out| RbDataFrame::from(out.1)))
        }))
    }
}

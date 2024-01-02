use magnus::{prelude::*, RArray, Value};
use polars::io::mmap::MmapBytesReader;
use polars::io::RowCount;
use polars::prelude::read_impl::OwnedBatchedCsvReader;
use polars::prelude::*;
use std::cell::RefCell;
use std::path::PathBuf;

use crate::conversion::*;
use crate::prelude::read_impl::OwnedBatchedCsvReaderMmap;
use crate::{RbDataFrame, RbPolarsErr, RbResult};

pub enum BatchedReader {
    MMap(OwnedBatchedCsvReaderMmap),
    Read(OwnedBatchedCsvReader),
}

#[magnus::wrap(class = "Polars::RbBatchedCsv")]
pub struct RbBatchedCsv {
    pub reader: RefCell<BatchedReader>,
}

impl RbBatchedCsv {
    pub fn new(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let infer_schema_length = Option::<usize>::try_convert(arguments[0])?;
        let chunk_size = usize::try_convert(arguments[1])?;
        let has_header = bool::try_convert(arguments[2])?;
        let ignore_errors = bool::try_convert(arguments[3])?;
        let n_rows = Option::<usize>::try_convert(arguments[4])?;
        let skip_rows = usize::try_convert(arguments[5])?;
        let projection = Option::<Vec<usize>>::try_convert(arguments[6])?;
        let separator = String::try_convert(arguments[7])?;
        let rechunk = bool::try_convert(arguments[8])?;
        let columns = Option::<Vec<String>>::try_convert(arguments[9])?;
        let encoding = Wrap::<CsvEncoding>::try_convert(arguments[10])?;
        let n_threads = Option::<usize>::try_convert(arguments[11])?;
        let path = PathBuf::try_convert(arguments[12])?;
        let overwrite_dtype = Option::<Vec<(String, Wrap<DataType>)>>::try_convert(arguments[13])?;
        // TODO fix
        let overwrite_dtype_slice = Option::<Vec<Wrap<DataType>>>::None; // Option::<Vec<Wrap<DataType>>>::try_convert(arguments[14])?;
        let low_memory = bool::try_convert(arguments[15])?;
        let comment_prefix = Option::<String>::try_convert(arguments[16])?;
        let quote_char = Option::<String>::try_convert(arguments[17])?;
        let null_values = Option::<Wrap<NullValues>>::try_convert(arguments[18])?;
        let try_parse_dates = bool::try_convert(arguments[19])?;
        let skip_rows_after_header = usize::try_convert(arguments[20])?;
        let row_count = Option::<(String, IdxSize)>::try_convert(arguments[21])?;
        let sample_size = usize::try_convert(arguments[22])?;
        let eol_char = String::try_convert(arguments[23])?;
        // end arguments

        let null_values = null_values.map(|w| w.0);
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
            overwrite_dtype
                .iter()
                .map(|(name, dtype)| {
                    let dtype = dtype.0.clone();
                    Field::new(name, dtype)
                })
                .collect::<Schema>()
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
            .with_separator(separator.as_bytes()[0])
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
            .with_comment_prefix(comment_prefix.as_deref())
            .with_null_values(null_values)
            .with_try_parse_dates(try_parse_dates)
            .with_quote_char(quote_char)
            .with_end_of_line_char(eol_char)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_row_count(row_count)
            .sample_size(sample_size);

        let reader = if low_memory {
            let reader = reader
                .batched_read(overwrite_dtype.map(Arc::new))
                .map_err(RbPolarsErr::from)?;
            BatchedReader::Read(reader)
        } else {
            let reader = reader
                .batched_mmap(overwrite_dtype.map(Arc::new))
                .map_err(RbPolarsErr::from)?;
            BatchedReader::MMap(reader)
        };

        Ok(RbBatchedCsv {
            reader: RefCell::new(reader),
        })
    }

    pub fn next_batches(&self, n: usize) -> RbResult<Option<RArray>> {
        let batches = match &mut *self.reader.borrow_mut() {
            BatchedReader::MMap(reader) => reader.next_batches(n),
            BatchedReader::Read(reader) => reader.next_batches(n),
        }
        .map_err(RbPolarsErr::from)?;

        Ok(batches.map(|batches| RArray::from_iter(batches.into_iter().map(RbDataFrame::from))))
    }
}

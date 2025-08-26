use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::Mutex;

use magnus::{RArray, Ruby, Value, prelude::*};
use polars::io::RowIndex;
use polars::io::csv::read::OwnedBatchedCsvReader;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;

use crate::conversion::*;
use crate::{RbDataFrame, RbPolarsErr, RbResult};

#[magnus::wrap(class = "Polars::RbBatchedCsv")]
pub struct RbBatchedCsv {
    pub reader: RefCell<Mutex<OwnedBatchedCsvReader>>,
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
        let overwrite_dtype_slice = Option::<Vec<Wrap<DataType>>>::try_convert(arguments[14])?;
        let low_memory = bool::try_convert(arguments[15])?;
        let comment_prefix = Option::<String>::try_convert(arguments[16])?;
        let quote_char = Option::<String>::try_convert(arguments[17])?;
        let null_values = Option::<Wrap<NullValues>>::try_convert(arguments[18])?;
        let missing_utf8_is_empty_string = bool::try_convert(arguments[19])?;
        let try_parse_dates = bool::try_convert(arguments[20])?;
        let skip_rows_after_header = usize::try_convert(arguments[21])?;
        let row_index = Option::<(String, IdxSize)>::try_convert(arguments[22])?;
        let eol_char = String::try_convert(arguments[23])?;
        let raise_if_empty = bool::try_convert(arguments[24])?;
        let truncate_ragged_lines = bool::try_convert(arguments[25])?;
        let decimal_comma = bool::try_convert(arguments[26])?;
        // end arguments

        let null_values = null_values.map(|w| w.0);
        let eol_char = eol_char.as_bytes()[0];
        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });
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
                    Field::new((&**name).into(), dtype)
                })
                .collect::<Schema>()
        });

        let overwrite_dtype_slice = overwrite_dtype_slice.map(|overwrite_dtype| {
            overwrite_dtype
                .iter()
                .map(|dt| dt.0.clone())
                .collect::<Vec<_>>()
        });

        let file = std::fs::File::open(path).map_err(RbPolarsErr::from)?;
        let reader = Box::new(file) as Box<dyn MmapBytesReader>;
        let reader = CsvReadOptions::default()
            .with_infer_schema_length(infer_schema_length)
            .with_has_header(has_header)
            .with_n_rows(n_rows)
            .with_skip_rows(skip_rows)
            .with_ignore_errors(ignore_errors)
            .with_projection(projection.map(Arc::new))
            .with_rechunk(rechunk)
            .with_chunk_size(chunk_size)
            .with_columns(columns.map(|x| x.into_iter().map(PlSmallStr::from_string).collect()))
            .with_n_threads(n_threads)
            .with_dtype_overwrite(overwrite_dtype_slice.map(Arc::new))
            .with_low_memory(low_memory)
            .with_skip_rows_after_header(skip_rows_after_header)
            .with_row_index(row_index)
            .with_raise_if_empty(raise_if_empty)
            .with_parse_options(
                CsvParseOptions::default()
                    .with_separator(separator.as_bytes()[0])
                    .with_encoding(encoding.0)
                    .with_missing_is_null(!missing_utf8_is_empty_string)
                    .with_comment_prefix(comment_prefix.as_deref())
                    .with_null_values(null_values)
                    .with_try_parse_dates(try_parse_dates)
                    .with_quote_char(quote_char)
                    .with_eol_char(eol_char)
                    .with_truncate_ragged_lines(truncate_ragged_lines)
                    .with_decimal_comma(decimal_comma),
            )
            .into_reader_with_file_handle(reader);

        let reader = reader
            .batched(overwrite_dtype.map(Arc::new))
            .map_err(RbPolarsErr::from)?;

        Ok(RbBatchedCsv {
            reader: RefCell::new(Mutex::new(reader)),
        })
    }

    pub fn next_batches(&self, n: usize) -> RbResult<Option<RArray>> {
        let reader = &self.reader;
        let batches = reader
            .borrow()
            .lock()
            .map_err(|e| RbPolarsErr::Other(e.to_string()))?
            .next_batches(n)
            .map_err(RbPolarsErr::from)?;

        Ok(batches.map(|batches| {
            Ruby::get()
                .unwrap()
                .ary_from_iter(batches.into_iter().map(RbDataFrame::from))
        }))
    }
}

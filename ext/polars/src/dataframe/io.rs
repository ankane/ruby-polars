use magnus::{Value, prelude::*};
use polars::io::RowIndex;
use polars::io::avro::AvroCompression;
use polars::prelude::*;
use std::io::BufWriter;
use std::num::NonZeroUsize;

use super::*;
use crate::conversion::*;
use crate::file::{
    get_file_like, get_mmap_bytes_reader, get_mmap_bytes_reader_and_path, read_if_bytesio,
};
use crate::{RbPolarsErr, RbResult};

impl RbDataFrame {
    pub fn read_csv(arguments: &[Value]) -> RbResult<Self> {
        // start arguments
        // this pattern is needed for more than 16
        let rb_f = arguments[0];
        let infer_schema_length = Option::<usize>::try_convert(arguments[1])?;
        let chunk_size = usize::try_convert(arguments[2])?;
        let has_header = bool::try_convert(arguments[3])?;
        let ignore_errors = bool::try_convert(arguments[4])?;
        let n_rows = Option::<usize>::try_convert(arguments[5])?;
        let skip_rows = usize::try_convert(arguments[6])?;
        let skip_lines = usize::try_convert(arguments[7])?;
        let projection = Option::<Vec<usize>>::try_convert(arguments[8])?;
        let separator = String::try_convert(arguments[9])?;
        let rechunk = bool::try_convert(arguments[10])?;
        let columns = Option::<Vec<String>>::try_convert(arguments[11])?;
        let encoding = Wrap::<CsvEncoding>::try_convert(arguments[12])?;
        let n_threads = Option::<usize>::try_convert(arguments[13])?;
        let path = Option::<String>::try_convert(arguments[14])?;
        let overwrite_dtype = Option::<Vec<(String, Wrap<DataType>)>>::try_convert(arguments[15])?;
        let overwrite_dtype_slice = Option::<Vec<Wrap<DataType>>>::try_convert(arguments[16])?;
        let low_memory = bool::try_convert(arguments[17])?;
        let comment_prefix = Option::<String>::try_convert(arguments[18])?;
        let quote_char = Option::<String>::try_convert(arguments[19])?;
        let null_values = Option::<Wrap<NullValues>>::try_convert(arguments[20])?;
        let missing_utf8_is_empty_string = bool::try_convert(arguments[21])?;
        let try_parse_dates = bool::try_convert(arguments[22])?;
        let skip_rows_after_header = usize::try_convert(arguments[23])?;
        let row_index = Option::<(String, IdxSize)>::try_convert(arguments[24])?;
        let eol_char = String::try_convert(arguments[25])?;
        let raise_if_empty = bool::try_convert(arguments[26])?;
        let truncate_ragged_lines = bool::try_convert(arguments[27])?;
        let decimal_comma = bool::try_convert(arguments[28])?;
        let schema = Option::<Wrap<Schema>>::try_convert(arguments[29])?;
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

        let rb_f = read_if_bytesio(rb_f);
        let mmap_bytes_r = get_mmap_bytes_reader(&rb_f)?;
        let df = CsvReadOptions::default()
            .with_path(path)
            .with_infer_schema_length(infer_schema_length)
            .with_has_header(has_header)
            .with_n_rows(n_rows)
            .with_skip_rows(skip_rows)
            .with_skip_lines(skip_lines)
            .with_ignore_errors(ignore_errors)
            .with_projection(projection.map(Arc::new))
            .with_rechunk(rechunk)
            .with_chunk_size(chunk_size)
            .with_columns(columns.map(|x| x.into_iter().map(|x| x.into()).collect()))
            .with_n_threads(n_threads)
            .with_schema_overwrite(overwrite_dtype.map(Arc::new))
            .with_dtype_overwrite(overwrite_dtype_slice.map(Arc::new))
            .with_schema(schema.map(|schema| Arc::new(schema.0)))
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
            .into_reader_with_file_handle(mmap_bytes_r)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(df.into())
    }

    pub fn read_json(
        rb_f: Value,
        infer_schema_length: Option<usize>,
        schema: Option<Wrap<Schema>>,
        schema_overrides: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let rb_f = read_if_bytesio(rb_f);
        let mmap_bytes_r = get_mmap_bytes_reader(&rb_f)?;

        let mut builder = JsonReader::new(mmap_bytes_r)
            .with_json_format(JsonFormat::Json)
            .infer_schema_len(infer_schema_length.and_then(NonZeroUsize::new));

        if let Some(schema) = schema {
            builder = builder.with_schema(Arc::new(schema.0));
        }

        if let Some(schema) = schema_overrides.as_ref() {
            builder = builder.with_schema_overwrite(&schema.0);
        }

        let out = builder.finish().map_err(RbPolarsErr::from)?;
        Ok(out.into())
    }

    pub fn read_ndjson(
        rb_f: Value,
        ignore_errors: bool,
        schema: Option<Wrap<Schema>>,
        schema_overrides: Option<Wrap<Schema>>,
    ) -> RbResult<Self> {
        let rb_f = read_if_bytesio(rb_f);
        let mmap_bytes_r = get_mmap_bytes_reader(&rb_f)?;

        let mut builder = JsonReader::new(mmap_bytes_r)
            .with_json_format(JsonFormat::JsonLines)
            .with_ignore_errors(ignore_errors);

        if let Some(schema) = schema {
            builder = builder.with_schema(Arc::new(schema.0));
        }

        if let Some(schema) = schema_overrides.as_ref() {
            builder = builder.with_schema_overwrite(&schema.0);
        }

        let out = builder
            .finish()
            .map_err(|e| RbPolarsErr::Other(format!("{e}")))?;
        Ok(out.into())
    }

    pub fn read_ipc(
        rb_f: Value,
        columns: Option<Vec<String>>,
        projection: Option<Vec<usize>>,
        n_rows: Option<usize>,
        row_index: Option<(String, IdxSize)>,
        memory_map: bool,
    ) -> RbResult<Self> {
        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });
        let rb_f = read_if_bytesio(rb_f);
        let (mmap_bytes_r, mmap_path) = get_mmap_bytes_reader_and_path(&rb_f)?;

        let mmap_path = if memory_map { mmap_path } else { None };
        let df = IpcReader::new(mmap_bytes_r)
            .with_projection(projection)
            .with_columns(columns)
            .with_n_rows(n_rows)
            .with_row_index(row_index)
            .memory_mapped(mmap_path)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn read_ipc_stream(
        rb_f: Value,
        columns: Option<Vec<String>>,
        projection: Option<Vec<usize>>,
        n_rows: Option<usize>,
        row_index: Option<(String, IdxSize)>,
        rechunk: bool,
    ) -> RbResult<Self> {
        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.into(),
            offset,
        });
        let rb_f = read_if_bytesio(rb_f);
        let mmap_bytes_r = get_mmap_bytes_reader(&rb_f)?;
        let df = IpcStreamReader::new(mmap_bytes_r)
            .with_projection(projection)
            .with_columns(columns)
            .with_n_rows(n_rows)
            .with_row_index(row_index)
            .set_rechunk(rechunk)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    pub fn read_avro(
        rb_f: Value,
        columns: Option<Vec<String>>,
        projection: Option<Vec<usize>>,
        n_rows: Option<usize>,
    ) -> RbResult<Self> {
        use polars::io::avro::AvroReader;

        let file = get_file_like(rb_f, false)?;
        let df = AvroReader::new(file)
            .with_projection(projection)
            .with_columns(columns)
            .with_n_rows(n_rows)
            .finish()
            .map_err(RbPolarsErr::from)?;
        Ok(RbDataFrame::new(df))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_csv(
        &self,
        rb_f: Value,
        include_header: bool,
        separator: u8,
        quote_char: u8,
        batch_size: NonZeroUsize,
        datetime_format: Option<String>,
        date_format: Option<String>,
        time_format: Option<String>,
        float_precision: Option<usize>,
        null_value: Option<String>,
    ) -> RbResult<()> {
        let null = null_value.unwrap_or_default();
        let mut buf = get_file_like(rb_f, true)?;
        CsvWriter::new(&mut buf)
            .include_header(include_header)
            .with_separator(separator)
            .with_quote_char(quote_char)
            .with_batch_size(batch_size)
            .with_datetime_format(datetime_format)
            .with_date_format(date_format)
            .with_time_format(time_format)
            .with_float_precision(float_precision)
            .with_null_value(null)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn write_parquet(
        &self,
        rb_f: Value,
        compression: String,
        compression_level: Option<i32>,
        statistics: Wrap<StatisticsOptions>,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
    ) -> RbResult<()> {
        let compression = parse_parquet_compression(&compression, compression_level)?;

        let buf = get_file_like(rb_f, true)?;
        ParquetWriter::new(buf)
            .with_compression(compression)
            .with_statistics(statistics.0)
            .with_row_group_size(row_group_size)
            .with_data_page_size(data_page_size)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn write_json(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);

        JsonWriter::new(file)
            .with_json_format(JsonFormat::Json)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn write_ndjson(&self, rb_f: Value) -> RbResult<()> {
        let file = BufWriter::new(get_file_like(rb_f, true)?);

        JsonWriter::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;

        Ok(())
    }

    pub fn write_ipc(
        &self,
        rb_f: Value,
        compression: Wrap<Option<IpcCompression>>,
        compat_level: RbCompatLevel,
        cloud_options: Option<Vec<(String, String)>>,
        retries: usize,
    ) -> RbResult<()> {
        let cloud_options = if let Ok(path) = String::try_convert(rb_f) {
            let cloud_options = parse_cloud_options(&path, cloud_options.unwrap_or_default())?;
            Some(cloud_options.with_max_retries(retries))
        } else {
            None
        };

        let f = crate::file::try_get_writeable(rb_f, cloud_options.as_ref())?;

        IpcWriter::new(f)
            .with_compression(compression.0)
            .with_compat_level(compat_level.0)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn write_ipc_stream(
        &self,
        rb_f: Value,
        compression: Wrap<Option<IpcCompression>>,
        compat_level: RbCompatLevel,
    ) -> RbResult<()> {
        let mut buf = get_file_like(rb_f, true)?;
        IpcStreamWriter::new(&mut buf)
            .with_compression(compression.0)
            .with_compat_level(compat_level.0)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }

    pub fn write_avro(
        &self,
        rb_f: Value,
        compression: Wrap<Option<AvroCompression>>,
        name: String,
    ) -> RbResult<()> {
        use polars::io::avro::AvroWriter;
        let mut buf = get_file_like(rb_f, true)?;
        AvroWriter::new(&mut buf)
            .with_compression(compression.0)
            .with_name(name)
            .finish(&mut self.df.borrow_mut())
            .map_err(RbPolarsErr::from)?;
        Ok(())
    }
}

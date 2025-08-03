use std::sync::Arc;

use magnus::{TryConvert, Value, value::ReprValue};
use polars::prelude::deletion::DeletionFilesList;
use polars::prelude::{
    CastColumnsPolicy, ColumnMapping, ExtraColumnsPolicy, MissingColumnsPolicy, PlSmallStr, Schema,
    UnifiedScanArgs,
};
use polars_io::{HiveOptions, RowIndex};
use polars_utils::IdxSize;
use polars_utils::plpath::PlPathRef;
use polars_utils::slice_enum::Slice;

use crate::RbResult;
use crate::prelude::Wrap;

/// Interface to `class ScanOptions` on the Ruby side
pub struct RbScanOptions(Value);

impl TryConvert for RbScanOptions {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self(ob))
    }
}

impl RbScanOptions {
    pub fn extract_unified_scan_args(
        &self,
        // For cloud_options init
        first_path: Option<PlPathRef>,
    ) -> RbResult<UnifiedScanArgs> {
        let row_index: Option<(Wrap<PlSmallStr>, IdxSize)> = self.0.funcall("row_index", ())?;
        let pre_slice: Option<(i64, usize)> = self.0.funcall("pre_slice", ())?;
        let cast_options: Wrap<CastColumnsPolicy> = self.0.funcall("cast_options", ())?;
        let extra_columns: Wrap<ExtraColumnsPolicy> = self.0.funcall("extra_columns", ())?;
        let missing_columns: Wrap<MissingColumnsPolicy> = self.0.funcall("missing_columns", ())?;
        let include_file_paths: Option<Wrap<PlSmallStr>> =
            self.0.funcall("include_file_paths", ())?;
        let glob: bool = self.0.funcall("glob", ())?;
        let hive_partitioning: Option<bool> = self.0.funcall("hive_partitioning", ())?;
        let hive_schema: Option<Wrap<Schema>> = self.0.funcall("hive_schema", ())?;
        let try_parse_hive_dates: bool = self.0.funcall("try_parse_hive_dates", ())?;
        let rechunk: bool = self.0.funcall("rechunk", ())?;
        let cache: bool = self.0.funcall("cache", ())?;
        let storage_options: Option<Vec<(String, String)>> =
            self.0.funcall("storage_options", ())?;
        let retries: usize = self.0.funcall("retries", ())?;
        let deletion_files: Option<Wrap<DeletionFilesList>> =
            self.0.funcall("deletion_files", ())?;
        let column_mapping: Option<Wrap<ColumnMapping>> = self.0.funcall("column_mapping", ())?;

        let cloud_options = storage_options;

        let cloud_options = if let Some(first_path) = first_path {
            use crate::prelude::parse_cloud_options;

            let first_path_url = first_path.to_str();
            let cloud_options =
                parse_cloud_options(first_path_url, cloud_options.unwrap_or_default())?;

            Some(cloud_options.with_max_retries(retries))
        } else {
            None
        };

        let hive_schema = hive_schema.map(|s| Arc::new(s.0));

        let row_index = row_index.map(|(name, offset)| RowIndex {
            name: name.0,
            offset,
        });

        let hive_options = HiveOptions {
            enabled: hive_partitioning,
            hive_start_idx: 0,
            schema: hive_schema,
            try_parse_dates: try_parse_hive_dates,
        };

        let unified_scan_args = UnifiedScanArgs {
            // Schema is currently still stored inside the options per scan type, but we do eventually
            // want to put it here instead.
            schema: None,
            cloud_options,
            hive_options,
            rechunk,
            cache,
            glob,
            projection: None,
            row_index,
            pre_slice: pre_slice.map(Slice::from),
            cast_columns_policy: cast_options.0,
            missing_columns_policy: missing_columns.0,
            extra_columns_policy: extra_columns.0,
            include_file_paths: include_file_paths.map(|x| x.0),
            deletion_files: DeletionFilesList::filter_empty(deletion_files.map(|x| x.0)),
            column_mapping: column_mapping.map(|x| x.0),
        };

        Ok(unified_scan_args)
    }
}

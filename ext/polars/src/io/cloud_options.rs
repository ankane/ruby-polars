use std::time::Duration;

use magnus::{RHash, TryConvert, Value, r_hash::ForEach, value::ReprValue};
use polars::prelude::CloudScheme;
use polars_io::cloud::{CloudOptions, CloudRetryConfig};
use polars_utils::total_ord::TotalOrdWrap;

use crate::utils::to_rb_err;
use crate::{RbResult, RbValueError};

pub struct OptRbCloudOptions(Value);

impl TryConvert for OptRbCloudOptions {
    fn try_convert(ob: Value) -> RbResult<Self> {
        Ok(Self(ob))
    }
}

impl OptRbCloudOptions {
    pub fn extract_opt_cloud_options(
        &self,
        cloud_scheme: Option<CloudScheme>,
        credential_provider: Option<Value>,
    ) -> RbResult<Option<CloudOptions>> {
        if self.0.is_nil() && credential_provider.is_none() {
            return Ok(None);
        }

        let mut storage_options: Vec<(String, String)> = vec![];
        let mut file_cache_ttl: u64 = 2;
        let mut retry_config = CloudRetryConfig::default();

        let storage_options_dict = Option::<RHash>::try_convert(self.0)?;

        if let Some(storage_options_dict) = storage_options_dict {
            storage_options.reserve(storage_options_dict.len());

            storage_options_dict.foreach(|key: Value, value: Value| {
                let key: String = key.funcall("to_s", ())?;

                macro_rules! expected_type {
                    ($key_name:expr, $type_name:expr) => {{
                        |_| {
                            let key_name = $key_name;
                            let type_name = $type_name;
                            RbValueError::new_err(format!(
                                "invalid value for '{key_name}': '{value}' (expected {type_name})"
                            ))
                        }
                    }};
                }

                match &*key {
                    "file_cache_ttl" => {
                        file_cache_ttl = u64::try_convert(value)
                            .map_err(expected_type!("file_cache_ttl", "int"))?;
                    }
                    "max_retries" => {
                        retry_config.max_retries = Option::<usize>::try_convert(value)
                            .map_err(expected_type!("max_retries", "int"))?;
                    }
                    "retry_timeout_ms" => {
                        retry_config.retry_timeout = Some(Duration::from_millis(
                            u64::try_convert(value)
                                .map_err(expected_type!("retry_timeout", "int"))?,
                        ));
                    }
                    "retry_init_backoff_ms" => {
                        retry_config.retry_init_backoff = Some(Duration::from_millis(
                            u64::try_convert(value)
                                .map_err(expected_type!("retry_init_backoff", "int"))?,
                        ));
                    }
                    "retry_max_backoff_ms" => {
                        retry_config.retry_max_backoff = Some(Duration::from_millis(
                            u64::try_convert(value)
                                .map_err(expected_type!("retry_max_backoff", "int"))?,
                        ));
                    }
                    "retry_base_multiplier" => {
                        retry_config.retry_base_multiplier = Some(TotalOrdWrap(
                            f64::try_convert(value)
                                .map_err(expected_type!("retry_base_multiplier", "float"))?,
                        ));
                    }
                    _ => {
                        let value =
                            String::try_convert(value).map_err(expected_type!(&key, "str"))?;
                        storage_options.push((key, value))
                    }
                }

                Ok(ForEach::Continue)
            })?;
        }

        let mut cloud_options = CloudOptions::from_untyped_config(cloud_scheme, storage_options)
            .map_err(to_rb_err)?
            .with_retry_config(retry_config);

        if file_cache_ttl > 0 {
            cloud_options.file_cache_ttl = file_cache_ttl;
        }

        Ok(Some(cloud_options))
    }
}

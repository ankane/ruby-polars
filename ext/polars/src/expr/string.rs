use polars::prelude::*;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn str_concat(&self, delimiter: String) -> Self {
        self.inner.clone().str().concat(&delimiter).into()
    }

    pub fn str_to_date(
        &self,
        format: Option<String>,
        strict: bool,
        exact: bool,
        cache: bool,
    ) -> Self {
        let options = StrptimeOptions {
            format,
            strict,
            exact,
            cache,
            ..Default::default()
        };
        self.inner.clone().str().to_date(options).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn str_to_datetime(
        &self,
        format: Option<String>,
        time_unit: Option<Wrap<TimeUnit>>,
        time_zone: Option<TimeZone>,
        strict: bool,
        exact: bool,
        cache: bool,
        utc: bool,
        tz_aware: bool,
    ) -> Self {
        let options = StrptimeOptions {
            format,
            strict,
            exact,
            cache,
            tz_aware,
            utc,
        };
        self.inner
            .clone()
            .str()
            .to_datetime(time_unit.map(|tu| tu.0), time_zone, options)
            .into()
    }

    pub fn str_to_time(
        &self,
        format: Option<String>,
        strict: bool,
        cache: bool,
    ) -> Self {
        let options = StrptimeOptions {
            format,
            strict,
            cache,
            exact: true,
            ..Default::default()
        };
        self.inner.clone().str().to_time(options).into()
    }
}

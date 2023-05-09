use polars::prelude::*;

use crate::conversion::Wrap;
use crate::RbExpr;

impl RbExpr {
    pub fn dt_to_string(&self, format: String) -> Self {
        self.inner.clone().dt().to_string(&format).into()
    }

    pub fn dt_offset_by(&self, by: String) -> Self {
        let by = Duration::parse(&by);
        self.inner.clone().dt().offset_by(by).into()
    }

    pub fn dt_epoch_seconds(&self) -> Self {
        self.clone()
            .inner
            .map(
                |s| {
                    s.timestamp(TimeUnit::Milliseconds)
                        .map(|ca| Some((ca / 1000).into_series()))
                },
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn dt_with_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().with_time_unit(tu.0).into()
    }

    pub fn dt_convert_time_zone(&self, tz: TimeZone) -> Self {
        self.inner.clone().dt().convert_time_zone(tz).into()
    }

    pub fn dt_cast_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().cast_time_unit(tu.0).into()
    }

    pub fn dt_replace_time_zone(&self, tz: Option<String>, use_earliest: Option<bool>) -> Self {
        self.inner.clone().dt().replace_time_zone(tz, use_earliest).into()
    }

    #[allow(deprecated)]
    pub fn dt_tz_localize(&self, tz: String) -> Self {
        self.inner.clone().dt().tz_localize(tz).into()
    }

    pub fn dt_truncate(&self, every: String, offset: String) -> Self {
        self.inner.clone().dt().truncate(&every, &offset).into()
    }

    pub fn dt_month_start(&self) -> Self {
        self.inner.clone().dt().month_start().into()
    }

    pub fn dt_month_end(&self) -> Self {
        self.inner.clone().dt().month_end().into()
    }

    pub fn dt_round(&self, every: String, offset: String) -> Self {
        self.inner.clone().dt().round(&every, &offset).into()
    }

    pub fn dt_combine(&self, time: &Self, time_unit: Wrap<TimeUnit>) -> Self {
        self.inner
            .clone()
            .dt()
            .combine(time.inner.clone(), time_unit.0)
            .into()
    }

    pub fn dt_year(&self) -> Self {
        self.clone().inner.dt().year().into()
    }

    pub fn dt_is_leap_year(&self) -> Self {
        self.clone().inner.dt().is_leap_year().into()
    }

    pub fn dt_iso_year(&self) -> Self {
        self.clone().inner.dt().iso_year().into()
    }

    pub fn dt_quarter(&self) -> Self {
        self.clone().inner.dt().quarter().into()
    }

    pub fn dt_month(&self) -> Self {
        self.clone().inner.dt().month().into()
    }

    pub fn dt_week(&self) -> Self {
        self.clone().inner.dt().week().into()
    }

    pub fn dt_weekday(&self) -> Self {
        self.clone().inner.dt().weekday().into()
    }

    pub fn dt_day(&self) -> Self {
        self.clone().inner.dt().day().into()
    }

    pub fn dt_ordinal_day(&self) -> Self {
        self.clone().inner.dt().ordinal_day().into()
    }

    pub fn dt_time(&self) -> Self {
        self.clone().inner.dt().time().into()
    }

    pub fn dt_date(&self) -> Self {
        self.clone().inner.dt().date().into()
    }

    pub fn dt_datetime(&self) -> Self {
        self.clone().inner.dt().datetime().into()
    }

    pub fn dt_hour(&self) -> Self {
        self.clone().inner.dt().hour().into()
    }

    pub fn dt_minute(&self) -> Self {
        self.clone().inner.dt().minute().into()
    }

    pub fn dt_second(&self) -> Self {
        self.clone().inner.dt().second().into()
    }

    pub fn dt_millisecond(&self) -> Self {
        self.clone().inner.dt().millisecond().into()
    }

    pub fn dt_microsecond(&self) -> Self {
        self.clone().inner.dt().microsecond().into()
    }

    pub fn dt_nanosecond(&self) -> Self {
        self.clone().inner.dt().nanosecond().into()
    }

    pub fn dt_timestamp(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().timestamp(tu.0).into()
    }

    pub fn duration_days(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.days().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_hours(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.hours().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_minutes(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.minutes().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_seconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.seconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_milliseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.milliseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_microseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.microseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }

    pub fn duration_nanoseconds(&self) -> Self {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.nanoseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
}

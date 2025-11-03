use polars::prelude::*;

use crate::conversion::Wrap;
use crate::{RbExpr, RbPolarsErr, RbResult};

impl RbExpr {
    pub fn dt_add_business_days(
        &self,
        n: &RbExpr,
        week_mask: [bool; 7],
        holidays: Vec<i32>,
        roll: Wrap<Roll>,
    ) -> Self {
        self.inner
            .clone()
            .dt()
            .add_business_days(n.inner.clone(), week_mask, holidays, roll.0)
            .into()
    }

    pub fn dt_to_string(&self, format: String) -> Self {
        self.inner.clone().dt().to_string(&format).into()
    }

    pub fn dt_offset_by(&self, by: &RbExpr) -> Self {
        self.inner.clone().dt().offset_by(by.inner.clone()).into()
    }

    pub fn dt_with_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().with_time_unit(tu.0).into()
    }

    pub fn dt_convert_time_zone(&self, time_zone: String) -> RbResult<Self> {
        Ok(self
            .inner
            .clone()
            .dt()
            .convert_time_zone(
                TimeZone::opt_try_new(Some(PlSmallStr::from(time_zone)))
                    .map_err(RbPolarsErr::from)?
                    .unwrap_or(TimeZone::UTC),
            )
            .into())
    }

    pub fn dt_cast_time_unit(&self, tu: Wrap<TimeUnit>) -> Self {
        self.inner.clone().dt().cast_time_unit(tu.0).into()
    }

    pub fn dt_replace_time_zone(
        &self,
        time_zone: Option<String>,
        ambiguous: &Self,
        non_existent: Wrap<NonExistent>,
    ) -> RbResult<Self> {
        Ok(self
            .inner
            .clone()
            .dt()
            .replace_time_zone(
                TimeZone::opt_try_new(time_zone.map(PlSmallStr::from_string))
                    .map_err(RbPolarsErr::from)?,
                ambiguous.inner.clone(),
                non_existent.0,
            )
            .into())
    }

    pub fn dt_truncate(&self, every: &Self) -> Self {
        self.inner.clone().dt().truncate(every.inner.clone()).into()
    }

    pub fn dt_month_start(&self) -> Self {
        self.inner.clone().dt().month_start().into()
    }

    pub fn dt_month_end(&self) -> Self {
        self.inner.clone().dt().month_end().into()
    }

    pub fn dt_base_utc_offset(&self) -> Self {
        self.inner.clone().dt().base_utc_offset().into()
    }

    pub fn dt_dst_offset(&self) -> Self {
        self.inner.clone().dt().dst_offset().into()
    }

    pub fn dt_round(&self, every: &Self) -> Self {
        self.inner.clone().dt().round(every.inner.clone()).into()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn dt_replace(
        &self,
        year: &Self,
        month: &Self,
        day: &Self,
        hour: &Self,
        minute: &Self,
        second: &Self,
        microsecond: &Self,
        ambiguous: &Self,
    ) -> Self {
        self.inner
            .clone()
            .dt()
            .replace(
                year.inner.clone(),
                month.inner.clone(),
                day.inner.clone(),
                hour.inner.clone(),
                minute.inner.clone(),
                second.inner.clone(),
                microsecond.inner.clone(),
                ambiguous.inner.clone(),
            )
            .into()
    }

    pub fn dt_combine(&self, time: &Self, time_unit: Wrap<TimeUnit>) -> Self {
        self.inner
            .clone()
            .dt()
            .combine(time.inner.clone(), time_unit.0)
            .into()
    }

    pub fn dt_millennium(&self) -> Self {
        self.inner.clone().dt().millennium().into()
    }

    pub fn dt_century(&self) -> Self {
        self.inner.clone().dt().century().into()
    }

    pub fn dt_year(&self) -> Self {
        self.clone().inner.dt().year().into()
    }

    pub fn dt_is_business_day(&self, week_mask: [bool; 7], holidays: Vec<i32>) -> Self {
        self.inner
            .clone()
            .dt()
            .is_business_day(week_mask, holidays)
            .into()
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

    pub fn dt_total_days(&self, fractional: bool) -> Self {
        self.inner.clone().dt().total_days(fractional).into()
    }

    pub fn dt_total_hours(&self, fractional: bool) -> Self {
        self.inner.clone().dt().total_hours(fractional).into()
    }

    pub fn dt_total_minutes(&self, fractional: bool) -> Self {
        self.inner.clone().dt().total_minutes(fractional).into()
    }

    pub fn dt_total_seconds(&self, fractional: bool) -> Self {
        self.inner.clone().dt().total_seconds(fractional).into()
    }

    pub fn dt_total_milliseconds(&self, fractional: bool) -> Self {
        self.inner
            .clone()
            .dt()
            .total_milliseconds(fractional)
            .into()
    }

    pub fn dt_total_microseconds(&self, fractional: bool) -> Self {
        self.inner
            .clone()
            .dt()
            .total_microseconds(fractional)
            .into()
    }

    pub fn dt_total_nanoseconds(&self, fractional: bool) -> Self {
        self.inner.clone().dt().total_nanoseconds(fractional).into()
    }
}

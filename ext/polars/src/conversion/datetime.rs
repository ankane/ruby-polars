//! Utilities for converting dates, times, datetimes, and so on.

use std::str::FromStr;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, TimeDelta, TimeZone as _};
use chrono_tz::Tz;
use magnus::{IntoValue, Ruby, Value, prelude::*};
use polars::prelude::*;

use crate::rb_modules::pl_utils;
use crate::{RbPolarsErr, RbResult};

pub fn elapsed_offset_to_timedelta(elapsed: i64, time_unit: TimeUnit) -> TimeDelta {
    let (in_second, nano_multiplier) = match time_unit {
        TimeUnit::Nanoseconds => (1_000_000_000, 1),
        TimeUnit::Microseconds => (1_000_000, 1_000),
        TimeUnit::Milliseconds => (1_000, 1_000_000),
    };
    let mut elapsed_sec = elapsed / in_second;
    let mut elapsed_nanos = nano_multiplier * (elapsed % in_second);
    if elapsed_nanos < 0 {
        // TimeDelta expects nanos to always be positive.
        elapsed_sec -= 1;
        elapsed_nanos += 1_000_000_000;
    }
    TimeDelta::new(elapsed_sec, elapsed_nanos as u32).unwrap()
}

/// Convert time-units-since-epoch to a more structured object.
pub fn timestamp_to_naive_datetime(since_epoch: i64, time_unit: TimeUnit) -> NaiveDateTime {
    DateTime::UNIX_EPOCH.naive_utc() + elapsed_offset_to_timedelta(since_epoch, time_unit)
}

pub fn datetime_to_rb_object(
    ruby: &Ruby,
    v: i64,
    tu: TimeUnit,
    tz: Option<&TimeZone>,
) -> RbResult<Value> {
    if let Some(time_zone) = tz {
        if let Ok(tz) = Tz::from_str(time_zone) {
            let utc_datetime = DateTime::UNIX_EPOCH + elapsed_offset_to_timedelta(v, tu);
            if utc_datetime.year() >= 2100 {
                // chrono-tz does not support dates after 2100
                // https://github.com/chronotope/chrono-tz/issues/135
                pl_utils(&ruby).funcall("_to_ruby_datetime", (v, tu.to_ascii(), time_zone.as_str()))
            } else {
                let datetime = utc_datetime.with_timezone(&tz);
                Ok(datetime.fixed_offset().into_value_with(&ruby))
            }
        } else if let Ok(tz) = FixedOffset::from_str(time_zone) {
            let naive_datetime = timestamp_to_naive_datetime(v, tu);
            let datetime = tz.from_utc_datetime(&naive_datetime);
            Ok(datetime.into_value_with(&ruby))
        } else {
            Err(RbPolarsErr::Other(format!("Could not parse timezone: {time_zone}")).into())
        }
    } else {
        Ok(timestamp_to_naive_datetime(v, tu)
            .and_utc()
            .into_value_with(&ruby))
    }
}

//! The daily trigger.
//!
//! Requires the `size_trigger` feature.

use chrono::{Datelike, Days, Local, LocalResult, NaiveTime, TimeZone, Timelike};

use crate::append::rolling_file::{policy::compound::trigger::Trigger, LogFile};

#[cfg(feature = "config_parsing")]
use crate::config::{Deserialize, Deserializers};

/// Configuration for the daily trigger.
#[cfg(feature = "config_parsing")]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DailyTriggerConfig {
    time_of_day: u32,
    skip_days: u32,
    start_day_of_week: u32,
}

/// A trigger which rolls the log on a daily basis.
#[derive(Debug, Default)]
struct DailyTrigger {
    next_seconds: std::sync::atomic::AtomicI64,
    time_of_day: NaiveTime,
    skip_days: u32,
    start_day_of_week: u32,
}

impl DailyTrigger {
    fn first_trigger_point(&self) -> i64 {
        let now = Local::now();
        let now_time = now.time();
        let now_day_of_week = now.weekday().num_days_from_sunday();
        let days_into_cycle = if now_day_of_week >= self.start_day_of_week {
            (now_day_of_week - self.start_day_of_week) % (self.skip_days + 1)
        } else {
            (now_day_of_week + 7 - self.start_day_of_week) % (self.skip_days + 1)
        };
        let days_left_in_cycle = (self.skip_days + 1) - days_into_cycle;
        let trigger_point = if now_time < self.time_of_day && days_into_cycle == 0 {
            now
        } else {
            now.checked_add_days(Days::new(days_left_in_cycle as u64))
                .expect("There is no tomorrow?")
        };
        trigger_point
            .with_hour(self.time_of_day.hour())
            .expect("There is no hour?")
            .with_minute(self.time_of_day.minute())
            .expect("There is no minute?")
            .with_second(0)
            .expect("There is no second?")
            .timestamp()
    }

    /// Returns a new trigger which rolls log the on a daily schedule.
    fn new(time_of_day: u32, skip_days: u32, start_day_of_week: u32) -> Self {
        let mut result: Self = Default::default();
        let hours = (time_of_day / 100) % 24 + (time_of_day % 100) / 60;
        let minutes = (time_of_day % 100) % 60;
        result.time_of_day =
            NaiveTime::from_hms_opt(hours, minutes, 0).expect("There is no such time?");
        result.start_day_of_week = start_day_of_week % 7;
        result.skip_days = skip_days;
        result.next_seconds.store(
            result.first_trigger_point(),
            std::sync::atomic::Ordering::Relaxed,
        );
        result
    }
}

impl Trigger for DailyTrigger {
    fn trigger(&self, _: &LogFile) -> anyhow::Result<bool> {
        let now = Local::now().timestamp();
        let next = self.next_seconds.load(std::sync::atomic::Ordering::SeqCst);
        if now < next {
            return Ok(false);
        }
        let last = match Local.timestamp_opt(next, 0) {
            LocalResult::Single(ts) => ts,
            // if we rotated in the middle of a DST change, the last one could be ambiguous,
            // so we just pick one of the two.
            LocalResult::Ambiguous(ts1, _) => ts1,
            _ => panic!("The trigger time was invalid"),
        };
        let next = last
            .checked_add_days(Days::new(self.skip_days as u64 + 1))
            .expect("The next trigger time is invalid");
        self.next_seconds
            .store(next.timestamp(), std::sync::atomic::Ordering::SeqCst);
        Ok(true)
    }
}

/// A deserializer for the `DailyTrigger`.
///
/// # Configuration
///
/// ```yaml
/// kind: daily
///
/// # The time to do the rotate: this should be a 24-hour time
/// # written as a 4-digit integer (e.g., 0000 or 1430).  The
/// # default value is 0000 (midnight), so this is optional.
/// # (If you specify a value that is out of range, it will be fixed.
/// # Hours 24 and above will be taken mod 24.  Minutes
/// # 60-99 will be taken mod 60 and will bump the hour by 1.)
/// time_of_day: 0000
///
/// # The number of days to skip between daily rotations.  If you
/// # want rotations every day, this should be 0; for every other day,
/// # this should be 1.  The default value is 0, so this is optional.
/// skip_days: 0
///
/// # The day of week (with Sunday being 0) to start your rotation
/// # cycle.  This only matters if skip_days is non-zero.  For example,
/// # if you want a weekly rotation on Wednesday, set skip_days to 7
/// # and this value to 3.  The default value is 0, so this is optional.
/// # (Values out of range will be taken mod 7.)
/// start_day_of_week: 0
/// ```
#[cfg(feature = "config_parsing")]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct DailyTriggerDeserializer;

#[cfg(feature = "config_parsing")]
impl Deserialize for DailyTriggerDeserializer {
    type Trait = dyn Trigger;

    type Config = DailyTriggerConfig;

    fn deserialize(
        &self,
        config: DailyTriggerConfig,
        _: &Deserializers,
    ) -> anyhow::Result<Box<dyn Trigger>> {
        Ok(Box::new(DailyTrigger::new(
            config.time_of_day,
            config.skip_days,
            config.start_day_of_week,
        )))
    }
}
